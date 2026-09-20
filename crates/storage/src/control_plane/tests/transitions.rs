// Copyright The Argmin Authors.
// SPDX-License-Identifier: Apache-2.0

use super::*;
use crate::control_plane_command::ReplicatedControlPlaneStateMachine;

fn certified_spare_authority() -> (
    test_util::TempDir,
    FileControlPlaneStore,
    SingleAuthorityControlPlane<FileControlPlaneStore>,
    PgId,
) {
    certified_spare_authority_with_policy(
        4,
        test_certified_storage_placement_policy((1..=4).map(NodeId::new), 3, 50),
    )
}

fn certified_spare_authority_with_policy(
    node_count: u32,
    placement_policy: CertifiedStoragePlacementPolicy,
) -> (
    test_util::TempDir,
    FileControlPlaneStore,
    SingleAuthorityControlPlane<FileControlPlaneStore>,
    PgId,
) {
    let (tmp, store, authority, mut pg_ids) = certified_spare_authority_with_policy_and_pgs(
        node_count,
        placement_policy,
        vec![PgId::new(7)],
    );
    (tmp, store, authority, pg_ids.remove(0))
}

fn certified_spare_authority_with_policy_and_pgs(
    node_count: u32,
    placement_policy: CertifiedStoragePlacementPolicy,
    pg_ids: Vec<PgId>,
) -> (
    test_util::TempDir,
    FileControlPlaneStore,
    SingleAuthorityControlPlane<FileControlPlaneStore>,
    Vec<PgId>,
) {
    let tmp = test_util::tempdir();
    let store = FileControlPlaneStore::new(tmp.path().join("control-plane.state"));
    let nodes = (1..=node_count)
        .map(|node_id| {
            (
                NodeId::new(node_id),
                format!("/tmp/transition-node-{node_id}.sock"),
            )
        })
        .collect::<Vec<_>>();
    let pgs = pg_ids
        .iter()
        .copied()
        .map(|pg_id| (pg_id, vec![NodeId::new(1), NodeId::new(2), NodeId::new(3)]))
        .collect::<Vec<_>>();
    let topology = InitialClusterTopologyCertificate::new_for_bootstrap_map(
        3,
        [0x5a; CONTROL_PLANE_TOPOLOGY_DIGEST_LEN],
        (1..=u64::from(node_count)).collect(),
        &nodes,
        &pgs,
        placement_policy,
    )
    .unwrap();
    let snapshot = ClusterControlSnapshot::empty()
        .apply_control_plane_command(ControlPlaneCommand::BootstrapCertifiedInitialClusterMap {
            nodes,
            pg_acting_sets: pgs,
            topology,
        })
        .unwrap()
        .into_snapshot();
    store.checkpoint(None, &snapshot).unwrap();
    let authority = SingleAuthorityControlPlane::open(store.clone()).unwrap();
    (tmp, store, authority, pg_ids)
}

fn heartbeat_spare_node(
    authority: &mut SingleAuthorityControlPlane<FileControlPlaneStore>,
    node_id: u32,
    now_ms: u64,
) {
    let now_ms = authority
        .snapshot()
        .max_committed_timestamp_ms()
        .map_or(now_ms, |committed| committed.max(now_ms));
    let mut request = heartbeat(node_id, authority.snapshot().cluster_epoch(), now_ms);
    request.endpoint = format!("/tmp/transition-node-{node_id}.sock");
    request.requested_lease_duration_ms = 10_000;
    authority.heartbeat(request, now_ms).unwrap();
    let epoch = authority.snapshot().cluster_epoch();
    let mut request = heartbeat_from_record(authority, node_id, epoch, now_ms + 1);
    request.requested_lease_duration_ms = 10_000;
    authority.heartbeat(request, now_ms + 1).unwrap();
}

fn forge_staging_authorization_at_epoch(
    snapshot: &mut ClusterControlSnapshot,
    transitions: &[(PgId, ClusterEpoch)],
    receipt_epoch: ClusterEpoch,
) {
    forge_staging_authorization_at_epoch_with_length(
        snapshot,
        transitions,
        receipt_epoch,
        |pg_id| 4_096 + u64::from(pg_id.get()),
    );
}

fn forge_staging_authorization_at_epoch_with_length(
    snapshot: &mut ClusterControlSnapshot,
    transitions: &[(PgId, ClusterEpoch)],
    receipt_epoch: ClusterEpoch,
    artifact_length: impl Fn(PgId) -> u64,
) {
    let requests = transitions
        .iter()
        .copied()
        .map(|(pg_id, transition_epoch)| {
            let transition = snapshot
                .unavailable_pg_placement_transition(pg_id)
                .filter(|transition| transition.transition_epoch() == transition_epoch)
                .or_else(|| {
                    snapshot
                        .retained_unavailable_pg_placement_transitions()
                        .find(|transition| {
                            transition.pg_id() == pg_id
                                && transition.transition_epoch() == transition_epoch
                        })
                })
                .unwrap();
            UnavailablePgStagingIntentAuthorizationRequest {
                unavailable_transition: UnavailablePgTransitionMutationBinding::new(
                    transition.pg_id,
                    transition.transition_epoch,
                    transition.source_epoch,
                    transition.source_acting_set.clone(),
                    transition.destination_acting_set.clone(),
                ),
                staging_generation: transition.transition_epoch.get(),
                artifact_target_epoch: next_epoch(receipt_epoch).unwrap(),
                artifact_digest: [u8::try_from(pg_id.get()).unwrap(); 32],
                artifact_length: artifact_length(pg_id),
                artifact_format_version:
                    crate::pg_store::METADATA_TRANSFER_STAGED_ARTIFACT_FORMAT_VERSION,
            }
        })
        .collect::<Vec<_>>();
    let receipt = UnavailablePgTransitionBatchReceipt {
        identity: unavailable_pg_staging_authorization_batch_identity(&requests),
        source_epoch: receipt_epoch,
        target_epoch: receipt_epoch,
    };
    for request in requests {
        let pg_id = request.unavailable_transition.pg_id();
        let transition_epoch = request.unavailable_transition.transition_epoch();
        let is_active = snapshot
            .unavailable_pg_placement_transitions
            .contains_key(&pg_id);
        let transition = if is_active {
            snapshot
                .unavailable_pg_placement_transitions
                .get_mut(&pg_id)
                .unwrap()
        } else {
            snapshot
                .retained_unavailable_pg_placement_transitions
                .get_mut(&(pg_id, transition_epoch))
                .unwrap()
        };
        transition.staging_authorization = Some(UnavailablePgStagingIntentAuthorization {
            staging_generation: request.staging_generation,
            artifact_target_epoch: request.artifact_target_epoch,
            artifact_digest: request.artifact_digest,
            artifact_length: request.artifact_length,
            artifact_format_version: request.artifact_format_version,
            batch_receipt: receipt.clone(),
        });
    }
}

fn retain_staging_evidence_page_for_test(
    snapshot: &mut ClusterControlSnapshot,
    page: &crate::pg_store::MetadataTransferStagingEvidencePage,
) {
    for entry in page.entries() {
        let evidence = crate::pg_store::decode_staging_evidence(entry.evidence()).unwrap();
        snapshot.metadata_transfer_staging_evidence.insert(
            metadata_transfer_staging_evidence_key(&evidence),
            evidence.as_bytes().to_vec(),
        );
    }
    let receipt = crate::pg_store::MetadataTransferStagingEvidenceApplyReceipt::for_page(page);
    snapshot.metadata_transfer_staging_evidence_pages.insert(
        (
            page.actor().node_id(),
            page.actor().node_incarnation(),
            page.generation(),
        ),
        MetadataTransferStagingEvidencePageRecord {
            operation_payload: page.operation_payload().to_vec(),
            page_digest: page.page_digest(),
            apply_receipt: receipt.as_bytes().to_vec(),
        },
    );
}

fn heartbeat_with_pg_proofs_and_lease_duration(
    authority: &mut SingleAuthorityControlPlane<FileControlPlaneStore>,
    node_id: u32,
    pg_ids: &[PgId],
    state: PgState,
    metadata_proof: PgMetadataProof,
    now_ms: u64,
    requested_lease_duration_ms: u64,
) -> HeartbeatLease {
    let now_ms = authority
        .snapshot()
        .max_committed_timestamp_ms()
        .map_or(now_ms, |committed| committed.max(now_ms));
    let mut request = heartbeat_from_record(
        authority,
        node_id,
        authority.snapshot().cluster_epoch(),
        now_ms,
    );
    request.pg_observations = pg_ids
        .iter()
        .copied()
        .map(|pg_id| NodePgHeartbeatObservation {
            pg_id,
            state,
            metadata_proof,
            metadata_log_epoch: ClusterEpoch::INITIAL,
            pending_metadata_command: None,
        })
        .collect();
    request.requested_lease_duration_ms = requested_lease_duration_ms;
    authority.heartbeat(request, now_ms).unwrap()
}

fn begin_request_from_command(command: ControlPlaneCommand) -> UnavailablePgTransitionBeginRequest {
    let ControlPlaneCommand::BeginUnavailablePgPlacementTransitions {
        mut transitions, ..
    } = command
    else {
        panic!("unavailable transition builder returned the wrong command kind");
    };
    assert_eq!(transitions.len(), 1);
    transitions.remove(0)
}

fn completion_request_from_command(
    command: ControlPlaneCommand,
) -> (UnavailablePgTransitionCompletionRequest, u64) {
    let ControlPlaneCommand::CompleteUnavailablePgPlacementTransitions {
        ready_at_ms,
        mut transitions,
    } = command
    else {
        panic!("unavailable transition builder returned the wrong command kind");
    };
    assert_eq!(transitions.len(), 1);
    (transitions.remove(0), ready_at_ms)
}

pub(super) fn staged_two_pg_install_fixture() -> (
    ClusterControlSnapshot,
    Vec<UnavailablePgTransitionInstallRequest>,
) {
    staged_two_pg_install_fixture_with_proof(PgMetadataProof::current(23, 0x2323, 0x3434))
}

pub(super) fn staging_actor_closure_snapshot_fixture() -> ClusterControlSnapshot {
    let (_tmp, _store, authority, authorizations) =
        begun_two_pg_staging_authorization_authority_fixture_with_proof(PgMetadataProof::current(
            23, 0x2323, 0x3434,
        ));
    let authorized = authority
        .snapshot()
        .apply_control_plane_command(ControlPlaneCommand::AuthorizeUnavailablePgStagingIntents {
            authorizations: authorizations.clone(),
        })
        .unwrap()
        .into_snapshot();
    let authorization = &authorizations[0];
    let actor_node_id = authorization
        .unavailable_transition
        .destination_acting_set()[0];
    let actor_record = authorized.node(actor_node_id).unwrap();
    let old_actor = crate::pg_store::MetadataTransferStagingNodeIdentity::new(
        actor_node_id,
        actor_record.node_incarnation(),
        actor_record.endpoint().to_owned(),
    )
    .unwrap();
    let staging_tmp = test_util::tempdir();
    let limits = crate::pg_store::MetadataTransferStagingLimits::new(8, 1 << 20, 8 << 20).unwrap();
    let old_store = crate::pg_store::MetadataTransferStagingStore::open(
        staging_tmp.path(),
        old_actor.clone(),
        limits,
    )
    .unwrap();
    let intent = crate::pg_store::MetadataTransferStagingIntent::for_unavailable_transition(
        &authorization.unavailable_transition,
        authorization.artifact_digest,
        authorization.artifact_length,
        authorization.artifact_format_version,
    )
    .unwrap();
    old_store.tombstone(&intent).unwrap();
    let second_authorization = &authorizations[1];
    let second_intent = crate::pg_store::MetadataTransferStagingIntent::for_unavailable_transition(
        &second_authorization.unavailable_transition,
        second_authorization.artifact_digest,
        second_authorization.artifact_length,
        second_authorization.artifact_format_version,
    )
    .unwrap();
    let old_page = old_store.next_evidence_page().unwrap().unwrap();
    let old_committed = authorized
        .apply_control_plane_command(
            ControlPlaneCommand::ApplyMetadataTransferStagingEvidencePage {
                operation_payload: old_page.operation_payload().to_vec(),
                page_digest: old_page.page_digest(),
            },
        )
        .unwrap()
        .into_snapshot();
    drop(old_store);

    let next_actor = crate::pg_store::MetadataTransferStagingNodeIdentity::new(
        actor_node_id,
        old_actor.node_incarnation() + 1,
        format!("{}-restarted", old_actor.endpoint()),
    )
    .unwrap();
    let heartbeat_at_ms = old_committed.max_committed_timestamp_ms().unwrap_or(0).max(
        old_committed
            .node(actor_node_id)
            .and_then(NodeControlRecord::lease_deadline_ms)
            .unwrap_or(0),
    ) + 1;
    let mut heartbeat = heartbeat_from_snapshot(
        &old_committed,
        actor_node_id.as_u32(),
        old_committed.cluster_epoch(),
        heartbeat_at_ms,
    );
    heartbeat.node_incarnation = next_actor.node_incarnation();
    heartbeat.endpoint = next_actor.endpoint().to_owned();
    heartbeat.requested_lease_duration_ms = 10_000;
    let actor_advanced = old_committed
        .apply_control_plane_command(ControlPlaneCommand::RecordNodeHeartbeat {
            heartbeat,
            heartbeat_at_ms,
            lease_deadline_ms: heartbeat_at_ms + 10_000,
            lease_horizon_authority: None,
        })
        .unwrap()
        .into_snapshot();
    let rebound_store =
        crate::pg_store::MetadataTransferStagingStore::open(staging_tmp.path(), next_actor, limits)
            .unwrap();
    let rebound_page = rebound_store.next_evidence_page().unwrap().unwrap();
    let rebound_committed = actor_advanced
        .apply_control_plane_command(
            ControlPlaneCommand::ApplyMetadataTransferStagingEvidencePage {
                operation_payload: rebound_page.operation_payload().to_vec(),
                page_digest: rebound_page.page_digest(),
            },
        )
        .unwrap()
        .into_snapshot();
    rebound_store
        .record_evidence_apply_receipt(
            &rebound_page,
            &crate::pg_store::MetadataTransferStagingEvidenceApplyReceipt::for_page(&rebound_page),
        )
        .unwrap();
    rebound_store.tombstone(&second_intent).unwrap();
    let rebound_successor = rebound_store.next_evidence_page().unwrap().unwrap();
    rebound_committed
        .apply_control_plane_command(
            ControlPlaneCommand::ApplyMetadataTransferStagingEvidencePage {
                operation_payload: rebound_successor.operation_payload().to_vec(),
                page_digest: rebound_successor.page_digest(),
            },
        )
        .unwrap()
        .into_snapshot()
}

fn staged_two_pg_install_fixture_with_proof(
    proof: PgMetadataProof,
) -> (
    ClusterControlSnapshot,
    Vec<UnavailablePgTransitionInstallRequest>,
) {
    let (_tmp, _store, authority, requests) =
        staged_two_pg_install_authority_fixture_with_proof(proof);
    (authority.snapshot().clone(), requests)
}

fn staged_two_pg_install_authority_fixture_with_proof(
    proof: PgMetadataProof,
) -> (
    test_util::TempDir,
    FileControlPlaneStore,
    SingleAuthorityControlPlane<FileControlPlaneStore>,
    Vec<UnavailablePgTransitionInstallRequest>,
) {
    staged_install_authority_fixture_with_proof(vec![PgId::new(70), PgId::new(71)], proof)
}

fn staged_install_authority_fixture_with_proof(
    pg_ids: Vec<PgId>,
    proof: PgMetadataProof,
) -> (
    test_util::TempDir,
    FileControlPlaneStore,
    SingleAuthorityControlPlane<FileControlPlaneStore>,
    Vec<UnavailablePgTransitionInstallRequest>,
) {
    let (tmp, store, mut authority, authorizations) =
        begun_staging_authorization_authority_fixture_with_proof(pg_ids.clone(), proof);
    authority
        .authorize_unavailable_pg_staging_intents_batch(&authorizations)
        .unwrap();
    // Production staging binds the artifact proof to the authorized source
    // epoch, not the later authority epoch after the staging batch commits.
    let staged_transfer = PgMetadataTransferProof::new(
        authorizations[0].unavailable_transition.source_epoch(),
        proof,
    );

    let destination_nodes = authority
        .snapshot()
        .unavailable_pg_placement_transition(pg_ids[0])
        .unwrap()
        .destination_acting_set
        .iter()
        .copied()
        .collect::<BTreeSet<_>>();
    for node_id in destination_nodes {
        let node = authority.snapshot().node(node_id).unwrap();
        let actor = crate::pg_store::MetadataTransferStagingNodeIdentity::new(
            node_id,
            node.node_incarnation(),
            node.endpoint().to_owned(),
        )
        .unwrap();
        let mut previous_receipt = None;
        for authorization in &authorizations {
            let intent =
                crate::pg_store::MetadataTransferStagingIntent::for_unavailable_transition(
                    &authorization.unavailable_transition,
                    authorization.artifact_digest,
                    authorization.artifact_length,
                    authorization.artifact_format_version,
                )
                .unwrap();
            let page =
                crate::pg_store::metadata_transfer_staging_publication_evidence_page_for_test(
                    actor.clone(),
                    &intent,
                    staged_transfer,
                    previous_receipt.as_ref(),
                );
            let apply_receipt = ControlPlaneAdmin::apply_metadata_transfer_staging_evidence_page(
                &mut authority,
                page.operation_payload().to_vec(),
                page.page_digest(),
            )
            .unwrap();
            previous_receipt = Some(
                crate::pg_store::decode_staging_evidence_apply_receipt(&apply_receipt).unwrap(),
            );
        }
    }

    let snapshot = authority.snapshot();
    let destination_epoch = next_epoch(snapshot.cluster_epoch()).unwrap();
    let requests = authorizations
        .iter()
        .map(|authorization| {
            let transition = snapshot
                .unavailable_pg_placement_transition(authorization.unavailable_transition.pg_id())
                .unwrap();
            let mut publications = transition
                .destination_acting_set
                .iter()
                .copied()
                .map(|node_id| {
                    let node = snapshot.node(node_id).unwrap();
                    let key = MetadataTransferStagingEvidenceKey {
                        pg_id: transition.pg_id,
                        staging_generation: authorization.staging_generation,
                        actor_node_id: node_id,
                        actor_node_incarnation: node.node_incarnation(),
                        kind: crate::pg_store::MetadataTransferStagingEvidenceKind::Publication,
                        target_epoch: Some(destination_epoch),
                    };
                    UnavailablePgStagingPublicationBinding {
                        node_id,
                        node_incarnation: node.node_incarnation(),
                        endpoint: node.endpoint().to_owned(),
                        evidence_digest: checksum::sha256::digest(
                            &snapshot.metadata_transfer_staging_evidence[&key],
                        ),
                    }
                })
                .collect::<Vec<_>>();
            publications.sort_by_key(|publication| publication.node_id);
            UnavailablePgTransitionInstallRequest {
                unavailable_transition: authorization.unavailable_transition.clone(),
                transfer: staged_transfer,
                expected_destination_epoch: destination_epoch,
                publications,
            }
        })
        .collect();
    (tmp, store, authority, requests)
}

fn completed_staged_two_pg_authority_fixture() -> (
    test_util::TempDir,
    FileControlPlaneStore,
    SingleAuthorityControlPlane<FileControlPlaneStore>,
    Vec<UnavailablePgTransitionInstallRequest>,
) {
    completed_staged_authority_fixture(vec![PgId::new(70), PgId::new(71)])
}

fn completed_staged_authority_fixture(
    pg_ids: Vec<PgId>,
) -> (
    test_util::TempDir,
    FileControlPlaneStore,
    SingleAuthorityControlPlane<FileControlPlaneStore>,
    Vec<UnavailablePgTransitionInstallRequest>,
) {
    let (tmp, store, mut authority, installs) = staged_install_authority_fixture_with_proof(
        pg_ids,
        PgMetadataProof::current(23, 0x2323, 0x3434),
    );
    let destination_epoch = installs[0].expected_destination_epoch;
    authority
        .install_unavailable_pg_placement_transitions_batch(&installs, destination_epoch)
        .unwrap();
    let pg_ids = installs
        .iter()
        .map(|install| install.unavailable_transition.pg_id())
        .collect::<Vec<_>>();
    let destination_nodes = installs
        .iter()
        .flat_map(|install| {
            install
                .unavailable_transition
                .destination_acting_set()
                .iter()
                .copied()
        })
        .collect::<BTreeSet<_>>();
    let proof = installs[0].transfer.metadata_proof();
    let heartbeat_at_ms = authority
        .snapshot()
        .unavailable_pg_placement_transitions
        .values()
        .map(|transition| transition.grace_cutoff_ms)
        .max()
        .unwrap()
        .max(
            authority
                .snapshot()
                .max_committed_timestamp_ms()
                .unwrap_or(0),
        )
        + CONTROL_PLANE_CLOCK_SKEW_BUDGET_MS
        + 1;
    for (offset, node_id) in destination_nodes.into_iter().enumerate() {
        heartbeat_with_pg_proofs_and_lease_duration(
            &mut authority,
            node_id.as_u32(),
            &pg_ids,
            PgState::Peering,
            proof,
            heartbeat_at_ms + u64::try_from(offset).unwrap(),
            10_000,
        );
    }
    let ready_at_ms = authority.snapshot().max_committed_timestamp_ms().unwrap() + 1;
    let work = pg_ids
        .iter()
        .map(|pg_id| {
            UnavailablePgReconciliationWork::from_transition(
                authority
                    .snapshot()
                    .unavailable_pg_placement_transition(*pg_id)
                    .unwrap(),
                UnavailablePgReconciliationStage::PayloadReadiness,
            )
        })
        .collect::<Vec<_>>();
    authority
        .complete_unavailable_pg_placement_transition_batch(&work, ready_at_ms)
        .unwrap();
    (tmp, store, authority, installs)
}

fn finalized_staging_floor_authority_fixture() -> (
    test_util::TempDir,
    FileControlPlaneStore,
    SingleAuthorityControlPlane<FileControlPlaneStore>,
    Vec<UnavailablePgTransitionInstallRequest>,
    FinalizeMetadataTransferStagingGenerationRequest,
) {
    finalized_staging_floor_authority_fixture_with_isolated_checkpoints(false)
}

fn finalized_staging_floor_authority_fixture_with_isolated_checkpoints(
    isolated_checkpoints: bool,
) -> (
    test_util::TempDir,
    FileControlPlaneStore,
    SingleAuthorityControlPlane<FileControlPlaneStore>,
    Vec<UnavailablePgTransitionInstallRequest>,
    FinalizeMetadataTransferStagingGenerationRequest,
) {
    let (tmp, store, mut authority, installs) = completed_staged_two_pg_authority_fixture();
    let bindings = installs
        .iter()
        .map(|install| install.unavailable_transition.clone())
        .collect::<Vec<_>>();
    let transitions = bindings
        .iter()
        .map(|binding| {
            authority
                .snapshot()
                .retained_unavailable_pg_placement_transitions
                .get(&(binding.pg_id(), binding.transition_epoch()))
                .unwrap()
                .clone()
        })
        .collect::<Vec<_>>();
    let destination_nodes = transitions[0].destination_acting_set.clone();
    for node_id in destination_nodes.iter().copied() {
        let node = authority.snapshot().node(node_id).unwrap();
        let actor = crate::pg_store::MetadataTransferStagingNodeIdentity::new(
            node_id,
            node.node_incarnation(),
            node.endpoint().to_owned(),
        )
        .unwrap();
        let current_page = authority
            .snapshot()
            .metadata_transfer_staging_evidence_pages
            .range(
                (node_id, actor.node_incarnation(), 0)
                    ..=(node_id, actor.node_incarnation(), u64::MAX),
            )
            .next_back()
            .unwrap()
            .1;
        let mut previous =
            crate::pg_store::decode_staging_evidence_apply_receipt(&current_page.apply_receipt)
                .unwrap();
        for (binding, transition) in bindings.iter().zip(&transitions) {
            let authorization = transition.staging_authorization.as_ref().unwrap();
            let page = crate::pg_store::metadata_transfer_staging_evidence_page_for_test(
                actor.clone(),
                binding,
                authorization.artifact_digest,
                authorization.artifact_length,
                authorization.artifact_format_version,
                crate::pg_store::MetadataTransferStagingEvidenceKind::Tombstone,
                Some(&previous),
            );
            previous = crate::pg_store::decode_staging_evidence_apply_receipt(
                &ControlPlaneAdmin::apply_metadata_transfer_staging_evidence_page(
                    &mut authority,
                    page.operation_payload().to_vec(),
                    page.page_digest(),
                )
                .unwrap(),
            )
            .unwrap();
        }
        if isolated_checkpoints {
            for generation in [1, 3] {
                ControlPlaneAdmin::checkpoint_metadata_transfer_staging_evidence_pages(
                    &mut authority,
                    node_id,
                    actor.node_incarnation(),
                    generation,
                    generation,
                )
                .unwrap();
            }
        } else {
            ControlPlaneAdmin::checkpoint_metadata_transfer_staging_evidence_pages(
                &mut authority,
                node_id,
                actor.node_incarnation(),
                1,
                3,
            )
            .unwrap();
        }
    }
    let first_authorization = transitions[0].staging_authorization.as_ref().unwrap();
    let mut tombstones = destination_nodes
        .iter()
        .copied()
        .map(|node_id| {
            let node = authority.snapshot().node(node_id).unwrap();
            let key = MetadataTransferStagingEvidenceKey {
                pg_id: bindings[0].pg_id(),
                staging_generation: first_authorization.staging_generation,
                actor_node_id: node_id,
                actor_node_incarnation: node.node_incarnation(),
                kind: crate::pg_store::MetadataTransferStagingEvidenceKind::Tombstone,
                target_epoch: None,
            };
            MetadataTransferStagingTombstoneBinding {
                node_id,
                node_incarnation: node.node_incarnation(),
                endpoint: node.endpoint().to_owned(),
                evidence_digest: checksum::sha256::digest(
                    &authority.snapshot().metadata_transfer_staging_evidence[&key],
                ),
            }
        })
        .collect::<Vec<_>>();
    tombstones.sort_by_key(|tombstone| tombstone.node_id);
    let cleanup = FinalizeMetadataTransferStagingGenerationRequest {
        unavailable_transition: bindings[0].clone(),
        staging_generation: first_authorization.staging_generation,
        disposition: MetadataTransferStagingCleanupDisposition::Completed,
        tombstones,
    };
    ControlPlaneAdmin::finalize_metadata_transfer_staging_generation(
        &mut authority,
        cleanup.clone(),
    )
    .unwrap();
    (tmp, store, authority, installs, cleanup)
}

pub(super) fn finalized_staging_floor_snapshot_fixture() -> ClusterControlSnapshot {
    finalized_staging_floor_authority_fixture()
        .2
        .snapshot()
        .clone()
}

pub(super) fn collapsed_staging_checkpoint_snapshot_fixture() -> ClusterControlSnapshot {
    let (_tmp, _store, mut authority, _, _) =
        finalized_staging_floor_authority_fixture_with_isolated_checkpoints(true);
    let segments = authority
        .snapshot()
        .metadata_transfer_staging_evidence_checkpoint_segments
        .iter()
        .map(|(key, segment)| (*key, segment.last_generation))
        .collect::<Vec<_>>();
    for (key, last_generation) in segments {
        ControlPlaneAdmin::collapse_metadata_transfer_staging_evidence_checkpoint_segment(
            &mut authority,
            key.0,
            key.1,
            key.2,
            last_generation,
        )
        .unwrap();
    }
    authority.snapshot().clone()
}

fn adjacent_collapsed_staging_checkpoint_authority_fixture() -> (
    test_util::TempDir,
    FileControlPlaneStore,
    SingleAuthorityControlPlane<FileControlPlaneStore>,
) {
    let (tmp, store, mut authority, installs) = completed_staged_authority_fixture(vec![
        PgId::new(70),
        PgId::new(71),
        PgId::new(72),
        PgId::new(73),
    ]);
    let bindings = installs
        .iter()
        .map(|install| install.unavailable_transition.clone())
        .collect::<Vec<_>>();
    let transitions = bindings
        .iter()
        .map(|binding| {
            authority
                .snapshot()
                .retained_unavailable_pg_placement_transitions
                .get(&(binding.pg_id(), binding.transition_epoch()))
                .unwrap()
                .clone()
        })
        .collect::<Vec<_>>();
    let destination_nodes = transitions[0].destination_acting_set.clone();
    for node_id in destination_nodes.iter().copied() {
        let node = authority.snapshot().node(node_id).unwrap();
        let actor = crate::pg_store::MetadataTransferStagingNodeIdentity::new(
            node_id,
            node.node_incarnation(),
            node.endpoint().to_owned(),
        )
        .unwrap();
        let current_page = authority
            .snapshot()
            .metadata_transfer_staging_evidence_pages
            .range(
                (node_id, actor.node_incarnation(), 0)
                    ..=(node_id, actor.node_incarnation(), u64::MAX),
            )
            .next_back()
            .unwrap()
            .1;
        let mut previous =
            crate::pg_store::decode_staging_evidence_apply_receipt(&current_page.apply_receipt)
                .unwrap();
        for (binding, transition) in bindings.iter().zip(&transitions) {
            let authorization = transition.staging_authorization.as_ref().unwrap();
            let page = crate::pg_store::metadata_transfer_staging_evidence_page_for_test(
                actor.clone(),
                binding,
                authorization.artifact_digest,
                authorization.artifact_length,
                authorization.artifact_format_version,
                crate::pg_store::MetadataTransferStagingEvidenceKind::Tombstone,
                Some(&previous),
            );
            previous = crate::pg_store::decode_staging_evidence_apply_receipt(
                &ControlPlaneAdmin::apply_metadata_transfer_staging_evidence_page(
                    &mut authority,
                    page.operation_payload().to_vec(),
                    page.page_digest(),
                )
                .unwrap(),
            )
            .unwrap();
        }
        for generation in [1, 2, 3, 5, 6, 7] {
            ControlPlaneAdmin::checkpoint_metadata_transfer_staging_evidence_pages(
                &mut authority,
                node_id,
                actor.node_incarnation(),
                generation,
                generation,
            )
            .unwrap();
        }
    }

    for (binding, transition) in bindings.iter().zip(&transitions).take(3) {
        let authorization = transition.staging_authorization.as_ref().unwrap();
        let mut tombstones = destination_nodes
            .iter()
            .copied()
            .map(|node_id| {
                let node = authority.snapshot().node(node_id).unwrap();
                let key = MetadataTransferStagingEvidenceKey {
                    pg_id: binding.pg_id(),
                    staging_generation: authorization.staging_generation,
                    actor_node_id: node_id,
                    actor_node_incarnation: node.node_incarnation(),
                    kind: crate::pg_store::MetadataTransferStagingEvidenceKind::Tombstone,
                    target_epoch: None,
                };
                MetadataTransferStagingTombstoneBinding {
                    node_id,
                    node_incarnation: node.node_incarnation(),
                    endpoint: node.endpoint().to_owned(),
                    evidence_digest: checksum::sha256::digest(
                        &authority.snapshot().metadata_transfer_staging_evidence[&key],
                    ),
                }
            })
            .collect::<Vec<_>>();
        tombstones.sort_by_key(|tombstone| tombstone.node_id);
        ControlPlaneAdmin::finalize_metadata_transfer_staging_generation(
            &mut authority,
            FinalizeMetadataTransferStagingGenerationRequest {
                unavailable_transition: binding.clone(),
                staging_generation: authorization.staging_generation,
                disposition: MetadataTransferStagingCleanupDisposition::Completed,
                tombstones,
            },
        )
        .unwrap();
    }

    let segments = authority
        .snapshot()
        .metadata_transfer_staging_evidence_checkpoint_segments
        .iter()
        .map(|(key, segment)| (*key, segment.last_generation))
        .collect::<Vec<_>>();
    for (key, last_generation) in segments {
        ControlPlaneAdmin::collapse_metadata_transfer_staging_evidence_checkpoint_segment(
            &mut authority,
            key.0,
            key.1,
            key.2,
            last_generation,
        )
        .unwrap();
    }
    (tmp, store, authority)
}

#[test]
fn adjacent_checkpoint_anchors_coalesce_recursively_and_preserve_chain_tip() {
    let (_tmp, store, mut authority) = adjacent_collapsed_staging_checkpoint_authority_fixture();
    let first_key = *authority
        .snapshot()
        .metadata_transfer_staging_evidence_checkpoint_anchors
        .keys()
        .next()
        .unwrap();
    let actor_node_id = first_key.0;
    let actor_node_incarnation = first_key.1;
    let original_epoch = authority.snapshot().cluster_epoch();

    ControlPlaneAdmin::coalesce_metadata_transfer_staging_evidence_checkpoint_anchors(
        &mut authority,
        actor_node_id,
        actor_node_incarnation,
        1,
        2,
    )
    .unwrap();
    let twice = authority
        .snapshot()
        .metadata_transfer_staging_evidence_checkpoint_anchors
        .get(&(actor_node_id, actor_node_incarnation, 1))
        .unwrap();
    assert_eq!(twice.last_generation, 2);
    assert_eq!(twice.source_segment_count, 2);
    assert_eq!(twice.source_segment_digest, [0; 32]);
    authority
        .snapshot()
        .validate_current_state_invariants()
        .unwrap();

    ControlPlaneAdmin::coalesce_metadata_transfer_staging_evidence_checkpoint_anchors(
        &mut authority,
        actor_node_id,
        actor_node_incarnation,
        1,
        3,
    )
    .unwrap();
    let collapsed = authority.snapshot().clone();
    let thrice = collapsed
        .metadata_transfer_staging_evidence_checkpoint_anchors
        .get(&(actor_node_id, actor_node_incarnation, 1))
        .unwrap();
    assert_eq!(thrice.last_generation, 3);
    assert_eq!(thrice.source_segment_count, 3);
    assert_eq!(thrice.source_segment_digest, [0; 32]);
    assert_eq!(collapsed.cluster_epoch(), original_epoch);
    assert!(collapsed
        .metadata_transfer_staging_evidence_pages
        .contains_key(&(actor_node_id, actor_node_incarnation, 4)));
    collapsed.validate_current_state_invariants().unwrap();
    assert_eq!(
        parse_snapshot(&format_snapshot(&collapsed)).unwrap(),
        collapsed
    );

    let replay = ControlPlaneAdmin::coalesce_metadata_transfer_staging_evidence_checkpoint_anchors(
        &mut authority,
        actor_node_id,
        actor_node_incarnation,
        1,
        3,
    )
    .unwrap();
    assert_eq!(replay, collapsed);
    let absorbed_replay =
        ControlPlaneAdmin::coalesce_metadata_transfer_staging_evidence_checkpoint_anchors(
            &mut authority,
            actor_node_id,
            actor_node_incarnation,
            1,
            2,
        )
        .unwrap();
    assert_eq!(absorbed_replay, collapsed);

    drop(authority);
    let restarted = SingleAuthorityControlPlane::open(store).unwrap();
    assert_eq!(
        restarted
            .snapshot()
            .metadata_transfer_staging_evidence_checkpoint_anchors,
        collapsed.metadata_transfer_staging_evidence_checkpoint_anchors
    );
    assert_eq!(
        restarted
            .snapshot()
            .metadata_transfer_staging_finalized_floors,
        collapsed.metadata_transfer_staging_finalized_floors
    );
    restarted
        .snapshot()
        .validate_current_state_invariants()
        .unwrap();
}

#[test]
fn coalesced_checkpoint_anchor_does_not_block_a_successor_checkpoint() {
    let (_tmp, store, mut authority) = adjacent_collapsed_staging_checkpoint_authority_fixture();
    let first_key = *authority
        .snapshot()
        .metadata_transfer_staging_evidence_checkpoint_anchors
        .keys()
        .next()
        .unwrap();
    let metrics_before = observability::metadata_transfer_staging_retention_metrics_snapshot();

    ControlPlaneAdmin::coalesce_metadata_transfer_staging_evidence_checkpoint_anchors(
        &mut authority,
        first_key.0,
        first_key.1,
        1,
        3,
    )
    .unwrap();
    let metrics_after_coalesce =
        observability::metadata_transfer_staging_retention_metrics_snapshot();
    assert_eq!(
        metrics_after_coalesce.prune_applied_total,
        metrics_before.prune_applied_total + 1
    );
    assert_eq!(
        metrics_after_coalesce,
        observability::MetadataTransferStagingRetentionMetricSnapshot {
            prune_applied_total: metrics_after_coalesce.prune_applied_total,
            ..authority
                .snapshot()
                .metadata_transfer_staging_retention_metrics()
        }
    );
    let checkpointed = ControlPlaneAdmin::checkpoint_metadata_transfer_staging_evidence_pages(
        &mut authority,
        first_key.0,
        first_key.1,
        4,
        4,
    )
    .unwrap();
    let metrics_after_checkpoint =
        observability::metadata_transfer_staging_retention_metrics_snapshot();
    assert_eq!(
        metrics_after_checkpoint.prune_applied_total,
        metrics_before.prune_applied_total + 2
    );
    assert_eq!(
        metrics_after_checkpoint,
        observability::MetadataTransferStagingRetentionMetricSnapshot {
            prune_applied_total: metrics_after_checkpoint.prune_applied_total,
            ..checkpointed.metadata_transfer_staging_retention_metrics()
        }
    );

    assert!(checkpointed
        .metadata_transfer_staging_evidence_checkpoint_anchors
        .contains_key(&(first_key.0, first_key.1, 1)));
    assert!(checkpointed
        .metadata_transfer_staging_evidence_checkpoint_segments
        .contains_key(&(first_key.0, first_key.1, 4)));
    assert!(!checkpointed
        .metadata_transfer_staging_evidence_pages
        .contains_key(&(first_key.0, first_key.1, 4)));
    checkpointed.validate_current_state_invariants().unwrap();
    drop(authority);
    let restarted = SingleAuthorityControlPlane::open(store).unwrap();
    let metrics_after_restart =
        observability::metadata_transfer_staging_retention_metrics_snapshot();
    assert_eq!(
        metrics_after_restart,
        observability::MetadataTransferStagingRetentionMetricSnapshot {
            prune_applied_total: metrics_after_restart.prune_applied_total,
            ..restarted
                .snapshot()
                .metadata_transfer_staging_retention_metrics()
        }
    );
}

#[test]
fn checkpoint_anchor_coalescing_rejects_stale_or_forged_provenance_without_mutation() {
    let (_tmp, _store, authority) = adjacent_collapsed_staging_checkpoint_authority_fixture();
    let snapshot = authority.snapshot().clone();
    let first_key = *snapshot
        .metadata_transfer_staging_evidence_checkpoint_anchors
        .keys()
        .next()
        .unwrap();
    let command = snapshot
        .coalesce_metadata_transfer_staging_evidence_checkpoint_anchors_command(
            first_key.0,
            first_key.1,
            1,
            2,
        )
        .unwrap();
    let ControlPlaneCommand::CoalesceMetadataTransferStagingEvidenceCheckpointAnchors {
        actor_node_id,
        actor_node_incarnation,
        first_generation,
        last_generation,
        source_segment_count,
        mut source_segments_digest,
    } = command
    else {
        unreachable!("coalescing builder returned another command kind");
    };
    source_segments_digest[0] ^= 1;
    let error = snapshot
        .apply_control_plane_command(
            ControlPlaneCommand::CoalesceMetadataTransferStagingEvidenceCheckpointAnchors {
                actor_node_id,
                actor_node_incarnation,
                first_generation,
                last_generation,
                source_segment_count,
                source_segments_digest,
            },
        )
        .unwrap_err();
    assert!(error.to_string().contains("source segments do not match"));

    let error = snapshot
        .coalesce_metadata_transfer_staging_evidence_checkpoint_anchors_command(
            actor_node_id,
            actor_node_incarnation,
            1,
            4,
        )
        .unwrap_err();
    assert!(error.to_string().contains("do not cover the anchor"));

    let mut coalesced = snapshot
        .apply_control_plane_command(
            snapshot
                .coalesce_metadata_transfer_staging_evidence_checkpoint_anchors_command(
                    actor_node_id,
                    actor_node_incarnation,
                    1,
                    2,
                )
                .unwrap(),
        )
        .unwrap()
        .into_snapshot();
    let anchor = coalesced
        .metadata_transfer_staging_evidence_checkpoint_anchors
        .get_mut(&(actor_node_id, actor_node_incarnation, 1))
        .unwrap();
    anchor.source_segments_digest[0] ^= 1;
    assert!(coalesced
        .validate_current_state_invariants()
        .unwrap_err()
        .contains("cumulative source"));
}

fn checkpoint_anchor_direct_boundary_fixture() -> (
    test_util::TempDir,
    FileControlPlaneStore,
    SingleAuthorityControlPlane<FileControlPlaneStore>,
    crate::pg_store::MetadataTransferStagingNodeIdentity,
) {
    let (mut snapshot, installs) = staged_two_pg_install_fixture();
    snapshot.metadata_transfer_staging_evidence_pages.clear();
    snapshot.metadata_transfer_staging_evidence.clear();

    let first_binding = installs[0].unavailable_transition.clone();
    let second_binding = installs[1].unavailable_transition.clone();
    let first_transition = snapshot
        .unavailable_pg_placement_transition(first_binding.pg_id())
        .unwrap();
    let first_authorization = first_transition.staging_authorization.as_ref().unwrap();
    let first_intent = crate::pg_store::MetadataTransferStagingIntent::for_unavailable_transition(
        &first_binding,
        first_authorization.artifact_digest,
        first_authorization.artifact_length,
        first_authorization.artifact_format_version,
    )
    .unwrap();
    let second_transition = snapshot
        .unavailable_pg_placement_transition(second_binding.pg_id())
        .unwrap();
    let second_authorization = second_transition.staging_authorization.as_ref().unwrap();
    let second_intent = crate::pg_store::MetadataTransferStagingIntent::for_unavailable_transition(
        &second_binding,
        second_authorization.artifact_digest,
        second_authorization.artifact_length,
        second_authorization.artifact_format_version,
    )
    .unwrap();
    let staging_generation = first_authorization.staging_generation;
    let destination_epoch = installs[0].expected_destination_epoch;
    let destination_actors = first_binding
        .destination_acting_set()
        .iter()
        .copied()
        .map(|node_id| {
            let node = snapshot.node(node_id).unwrap();
            crate::pg_store::MetadataTransferStagingNodeIdentity::new(
                node_id,
                node.node_incarnation(),
                node.endpoint().to_owned(),
            )
            .unwrap()
        })
        .collect::<Vec<_>>();
    let boundary_actor = destination_actors[0].clone();

    let mut publication_targets = BTreeSet::from([destination_epoch]);
    let mut candidate = next_epoch(first_binding.transition_epoch()).unwrap();
    while publication_targets.len() < crate::pg_store::MAX_STAGING_EPOCH_PROOFS_PER_INTENT {
        publication_targets.insert(candidate);
        candidate = next_epoch(candidate).unwrap();
    }

    let mut previous_receipts = BTreeMap::new();
    let mut final_publications = Vec::new();
    for (index, actor) in destination_actors.iter().enumerate() {
        let actor_targets = if index == 0 {
            publication_targets.iter().copied().collect::<Vec<_>>()
        } else {
            vec![destination_epoch]
        };
        let mut previous = None;
        for target_epoch in actor_targets {
            let page = crate::pg_store::metadata_transfer_staging_publication_evidence_page_at_epoch_for_test(
                actor.clone(),
                &first_intent,
                target_epoch,
                installs[0].transfer,
                previous.as_ref(),
            );
            if target_epoch == destination_epoch {
                final_publications.push(UnavailablePgStagingPublicationBinding {
                    node_id: actor.node_id(),
                    node_incarnation: actor.node_incarnation(),
                    endpoint: actor.endpoint().to_owned(),
                    evidence_digest: checksum::sha256::digest(page.entries()[0].evidence()),
                });
            }
            let applied = snapshot
                .apply_control_plane_command(
                    ControlPlaneCommand::ApplyMetadataTransferStagingEvidencePage {
                        operation_payload: page.operation_payload().to_vec(),
                        page_digest: page.page_digest(),
                    },
                )
                .unwrap();
            let ControlPlaneCommandResponse::ApplyMetadataTransferStagingEvidencePage {
                apply_receipt,
            } = applied.response()
            else {
                unreachable!("staging evidence page returned another response kind");
            };
            previous = Some(
                crate::pg_store::decode_staging_evidence_apply_receipt(apply_receipt).unwrap(),
            );
            snapshot = applied.into_snapshot();
        }
        previous_receipts.insert(actor.node_id(), previous.unwrap());
    }
    final_publications.sort_by_key(|publication| publication.node_id);
    snapshot = snapshot
        .apply_control_plane_command(
            ControlPlaneCommand::InstallUnavailablePgPlacementTransitions {
                transitions: vec![UnavailablePgTransitionInstallRequest {
                    unavailable_transition: first_binding.clone(),
                    transfer: installs[0].transfer,
                    expected_destination_epoch: destination_epoch,
                    publications: final_publications,
                }],
                expected_destination_epoch: destination_epoch,
            },
        )
        .unwrap()
        .into_snapshot();

    let tmp = test_util::tempdir();
    let store = FileControlPlaneStore::new(tmp.path().join("control-plane.state"));
    store.checkpoint(None, &snapshot).unwrap();
    let mut authority = SingleAuthorityControlPlane::open(store.clone()).unwrap();
    let heartbeat_at_ms = authority
        .snapshot()
        .unavailable_pg_placement_transition(first_binding.pg_id())
        .unwrap()
        .grace_cutoff_ms
        .max(
            authority
                .snapshot()
                .max_committed_timestamp_ms()
                .unwrap_or(0),
        )
        + CONTROL_PLANE_CLOCK_SKEW_BUDGET_MS
        + 1;
    for (offset, actor) in destination_actors.iter().enumerate() {
        heartbeat_with_pg_proofs_and_lease_duration(
            &mut authority,
            actor.node_id().as_u32(),
            &[first_binding.pg_id()],
            PgState::Peering,
            installs[0].transfer.metadata_proof(),
            heartbeat_at_ms + u64::try_from(offset).unwrap(),
            10_000,
        );
    }
    let ready_at_ms = authority.snapshot().max_committed_timestamp_ms().unwrap() + 1;
    let work = vec![UnavailablePgReconciliationWork::from_transition(
        authority
            .snapshot()
            .unavailable_pg_placement_transition(first_binding.pg_id())
            .unwrap(),
        UnavailablePgReconciliationStage::PayloadReadiness,
    )];
    authority
        .complete_unavailable_pg_placement_transition_batch(&work, ready_at_ms)
        .unwrap();

    let successor_target_epoch = next_epoch(authority.snapshot().cluster_epoch()).unwrap();
    let mut tombstones = Vec::new();
    let mut checkpoint_ends = BTreeMap::new();
    for actor in &destination_actors {
        let previous = previous_receipts.get(&actor.node_id()).unwrap();
        let tombstone_page = crate::pg_store::metadata_transfer_staging_evidence_page_for_test(
            actor.clone(),
            &first_binding,
            first_intent.artifact_digest(),
            first_intent.artifact_length(),
            first_intent.artifact_format_version(),
            crate::pg_store::MetadataTransferStagingEvidenceKind::Tombstone,
            Some(previous),
        );
        tombstones.push(MetadataTransferStagingTombstoneBinding {
            node_id: actor.node_id(),
            node_incarnation: actor.node_incarnation(),
            endpoint: actor.endpoint().to_owned(),
            evidence_digest: checksum::sha256::digest(tombstone_page.entries()[0].evidence()),
        });
        let tombstone_generation = tombstone_page.generation();
        let tombstone_receipt = crate::pg_store::decode_staging_evidence_apply_receipt(
            &ControlPlaneAdmin::apply_metadata_transfer_staging_evidence_page(
                &mut authority,
                tombstone_page.operation_payload().to_vec(),
                tombstone_page.page_digest(),
            )
            .unwrap(),
        )
        .unwrap();
        let successor_page =
            crate::pg_store::metadata_transfer_staging_publication_evidence_page_at_epoch_for_test(
                actor.clone(),
                &second_intent,
                successor_target_epoch,
                installs[1].transfer,
                Some(&tombstone_receipt),
            );
        ControlPlaneAdmin::apply_metadata_transfer_staging_evidence_page(
            &mut authority,
            successor_page.operation_payload().to_vec(),
            successor_page.page_digest(),
        )
        .unwrap();
        checkpoint_ends.insert(actor.node_id(), tombstone_generation);
    }
    tombstones.sort_by_key(|tombstone| tombstone.node_id);

    for actor in &destination_actors {
        let last_generation = checkpoint_ends[&actor.node_id()];
        if actor == &boundary_actor {
            assert_eq!(
                last_generation,
                u64::try_from(MAX_METADATA_TRANSFER_STAGING_EVIDENCE_CHECKPOINT_COALESCED_ANCHORS)
                    .unwrap()
                    + 1
            );
            for generation in 1..=last_generation {
                ControlPlaneAdmin::checkpoint_metadata_transfer_staging_evidence_pages(
                    &mut authority,
                    actor.node_id(),
                    actor.node_incarnation(),
                    generation,
                    generation,
                )
                .unwrap();
            }
        } else {
            ControlPlaneAdmin::checkpoint_metadata_transfer_staging_evidence_pages(
                &mut authority,
                actor.node_id(),
                actor.node_incarnation(),
                1,
                last_generation,
            )
            .unwrap();
        }
    }
    ControlPlaneAdmin::finalize_metadata_transfer_staging_generation(
        &mut authority,
        FinalizeMetadataTransferStagingGenerationRequest {
            unavailable_transition: first_binding,
            staging_generation,
            disposition: MetadataTransferStagingCleanupDisposition::Completed,
            tombstones,
        },
    )
    .unwrap();

    let boundary_end = checkpoint_ends[&boundary_actor.node_id()];
    for generation in 1..=boundary_end {
        ControlPlaneAdmin::collapse_metadata_transfer_staging_evidence_checkpoint_segment(
            &mut authority,
            boundary_actor.node_id(),
            boundary_actor.node_incarnation(),
            generation,
            generation,
        )
        .unwrap();
    }
    authority
        .snapshot()
        .validate_current_state_invariants()
        .unwrap();
    (tmp, store, authority, boundary_actor)
}

#[test]
fn checkpoint_anchor_coalescing_enforces_direct_and_recursive_state_machine_bounds() {
    let (_tmp, _store, authority, actor) = checkpoint_anchor_direct_boundary_fixture();
    let snapshot = authority.snapshot().clone();
    let first_generation = 1;
    let last_generation =
        u64::try_from(MAX_METADATA_TRANSFER_STAGING_EVIDENCE_CHECKPOINT_COALESCED_ANCHORS + 1)
            .unwrap();
    let finalized_checkpoints = metadata_transfer_staging_finalized_checkpoint_index(
        &snapshot.metadata_transfer_staging_finalized_floors,
    )
    .unwrap();
    let all_sources = metadata_transfer_staging_checkpoint_source_segments(
        &finalized_checkpoints,
        actor.node_id(),
        actor.node_incarnation(),
        first_generation,
        last_generation,
    )
    .unwrap();
    assert_eq!(all_sources.len(), usize::try_from(last_generation).unwrap());
    let direct_ranges = snapshot
        .metadata_transfer_staging_evidence_checkpoint_anchors
        .range(
            (actor.node_id(), actor.node_incarnation(), 0)
                ..=(actor.node_id(), actor.node_incarnation(), u64::MAX),
        )
        .map(|(_, anchor)| (anchor.first_generation, anchor.last_generation))
        .collect::<Vec<_>>();
    assert_eq!(
        direct_ranges,
        (first_generation..=last_generation)
            .map(|generation| (generation, generation))
            .collect::<Vec<_>>()
    );
    let before = snapshot.clone();
    let error = snapshot
        .apply_control_plane_command(
            ControlPlaneCommand::CoalesceMetadataTransferStagingEvidenceCheckpointAnchors {
                actor_node_id: actor.node_id(),
                actor_node_incarnation: actor.node_incarnation(),
                first_generation,
                last_generation,
                source_segment_count: u64::try_from(all_sources.len()).unwrap(),
                source_segments_digest: metadata_transfer_staging_checkpoint_source_segments_digest(
                    &all_sources,
                ),
            },
        )
        .unwrap_err();
    assert!(
        error
            .to_string()
            .contains("exceeds the 64 direct-anchor limit"),
        "unexpected 65-anchor error: {error}"
    );
    assert_eq!(snapshot, before);

    let exact_end =
        u64::try_from(MAX_METADATA_TRANSFER_STAGING_EVIDENCE_CHECKPOINT_COALESCED_ANCHORS).unwrap();
    let exact_command = snapshot
        .coalesce_metadata_transfer_staging_evidence_checkpoint_anchors_command(
            actor.node_id(),
            actor.node_incarnation(),
            first_generation,
            exact_end,
        )
        .unwrap();
    let exact = snapshot
        .apply_control_plane_command(exact_command)
        .unwrap()
        .into_snapshot();
    assert_eq!(
        exact
            .metadata_transfer_staging_evidence_checkpoint_anchors
            .range(
                (actor.node_id(), actor.node_incarnation(), 0)
                    ..=(actor.node_id(), actor.node_incarnation(), u64::MAX),
            )
            .count(),
        2
    );

    let left_end = 32;
    let left = snapshot
        .apply_control_plane_command(
            snapshot
                .coalesce_metadata_transfer_staging_evidence_checkpoint_anchors_command(
                    actor.node_id(),
                    actor.node_incarnation(),
                    first_generation,
                    left_end,
                )
                .unwrap(),
        )
        .unwrap()
        .into_snapshot();
    let right = left
        .apply_control_plane_command(
            left.coalesce_metadata_transfer_staging_evidence_checkpoint_anchors_command(
                actor.node_id(),
                actor.node_incarnation(),
                left_end + 1,
                last_generation,
            )
            .unwrap(),
        )
        .unwrap()
        .into_snapshot();
    let recursive_command = right
        .coalesce_metadata_transfer_staging_evidence_checkpoint_anchors_command(
            actor.node_id(),
            actor.node_incarnation(),
            first_generation,
            last_generation,
        )
        .unwrap();
    let ControlPlaneCommand::CoalesceMetadataTransferStagingEvidenceCheckpointAnchors {
        source_segment_count,
        ..
    } = recursive_command
    else {
        unreachable!("coalescing builder returned another command kind");
    };
    assert_eq!(source_segment_count, last_generation);
    let recursive = right
        .apply_control_plane_command(recursive_command)
        .unwrap()
        .into_snapshot();
    let merged = recursive
        .metadata_transfer_staging_evidence_checkpoint_anchors
        .get(&(actor.node_id(), actor.node_incarnation(), first_generation))
        .unwrap();
    assert_eq!(merged.last_generation, last_generation);
    assert_eq!(merged.source_segment_count, last_generation);
    recursive.validate_current_state_invariants().unwrap();
}

#[test]
fn finalized_checkpoint_segments_collapse_to_exact_epoch_neutral_anchors() {
    let (_mixed_tmp, _mixed_store, mixed, _, _) = finalized_staging_floor_authority_fixture();
    let (&mixed_key, mixed_segment) = mixed
        .snapshot()
        .metadata_transfer_staging_evidence_checkpoint_segments
        .iter()
        .next()
        .unwrap();
    let mixed_error = mixed
        .snapshot()
        .apply_control_plane_command(
            ControlPlaneCommand::CollapseMetadataTransferStagingEvidenceCheckpointSegment {
                actor_node_id: mixed_key.0,
                actor_node_incarnation: mixed_key.1,
                first_generation: mixed_key.2,
                last_generation: mixed_segment.last_generation,
                source_segment_digest: metadata_transfer_staging_checkpoint_segment_digest(
                    mixed_segment,
                ),
            },
        )
        .unwrap_err();
    assert!(
        mixed_error
            .to_string()
            .contains("requires pruned detailed evidence"),
        "unexpected mixed-segment collapse error: {mixed_error}"
    );

    let (_tmp, store, mut authority, _, _) =
        finalized_staging_floor_authority_fixture_with_isolated_checkpoints(true);
    let original_epoch = authority.snapshot().cluster_epoch();
    let segments = authority
        .snapshot()
        .metadata_transfer_staging_evidence_checkpoint_segments
        .iter()
        .map(|(key, segment)| (*key, segment.last_generation))
        .collect::<Vec<_>>();
    assert!(!segments.is_empty());

    let (first_key, first_last_generation) = segments[0];
    let first_segment = &authority
        .snapshot()
        .metadata_transfer_staging_evidence_checkpoint_segments[&first_key];
    let mut stale_digest = metadata_transfer_staging_checkpoint_segment_digest(first_segment);
    stale_digest[0] ^= 1;
    let unchanged = authority.snapshot().clone();
    assert!(authority
        .snapshot()
        .apply_control_plane_command(
            ControlPlaneCommand::CollapseMetadataTransferStagingEvidenceCheckpointSegment {
                actor_node_id: first_key.0,
                actor_node_incarnation: first_key.1,
                first_generation: first_key.2,
                last_generation: first_last_generation,
                source_segment_digest: stale_digest,
            }
        )
        .unwrap_err()
        .to_string()
        .contains("source does not match"));
    assert_eq!(authority.snapshot(), &unchanged);

    for (key, last_generation) in &segments {
        ControlPlaneAdmin::collapse_metadata_transfer_staging_evidence_checkpoint_segment(
            &mut authority,
            key.0,
            key.1,
            key.2,
            *last_generation,
        )
        .unwrap();
    }
    let collapsed = authority.snapshot().clone();
    assert_eq!(collapsed.cluster_epoch(), original_epoch);
    assert!(collapsed
        .metadata_transfer_staging_evidence_checkpoint_segments
        .is_empty());
    assert_eq!(
        collapsed
            .metadata_transfer_staging_evidence_checkpoint_anchors
            .len(),
        segments.len()
    );
    collapsed.validate_current_state_invariants().unwrap();
    assert_eq!(
        parse_snapshot(&format_snapshot(&collapsed)).unwrap(),
        collapsed
    );

    for (key, last_generation) in &segments {
        let replay =
            ControlPlaneAdmin::collapse_metadata_transfer_staging_evidence_checkpoint_segment(
                &mut authority,
                key.0,
                key.1,
                key.2,
                *last_generation,
            )
            .unwrap();
        assert_eq!(replay, collapsed);
    }

    let mut forged_anchor = collapsed.clone();
    forged_anchor
        .metadata_transfer_staging_evidence_checkpoint_anchors
        .get_mut(&first_key)
        .unwrap()
        .source_segment_digest[0] ^= 1;
    assert!(parse_snapshot(&format_snapshot(&forged_anchor))
        .unwrap_err()
        .to_string()
        .contains("checkpoint anchor"));

    let mut forged_binding = collapsed.clone();
    let binding = forged_binding
        .metadata_transfer_staging_finalized_floors
        .values_mut()
        .flat_map(|floor| floor.checkpoint_bindings.values_mut())
        .find(|binding| {
            (
                binding.actor_node_id,
                binding.actor_node_incarnation,
                binding.first_generation,
            ) == first_key
        })
        .unwrap();
    binding.segment_digest[0] ^= 1;
    assert!(parse_snapshot(&format_snapshot(&forged_binding))
        .unwrap_err()
        .to_string()
        .contains("checkpoint anchor"));

    let mut coordinated_forgery = collapsed.clone();
    let anchor = coordinated_forgery
        .metadata_transfer_staging_evidence_checkpoint_anchors
        .get_mut(&first_key)
        .unwrap();
    let original_digest = anchor.source_segment_digest;
    anchor.source_segment_digest[0] ^= 1;
    let forged_digest = anchor.source_segment_digest;
    for binding in coordinated_forgery
        .metadata_transfer_staging_finalized_floors
        .values_mut()
        .flat_map(|floor| floor.checkpoint_bindings.values_mut())
        .filter(|binding| {
            (
                binding.actor_node_id,
                binding.actor_node_incarnation,
                binding.first_generation,
            ) == first_key
                && binding.segment_digest == original_digest
        })
    {
        binding.segment_digest = forged_digest;
    }
    assert!(parse_snapshot(&format_snapshot(&coordinated_forgery))
        .unwrap_err()
        .to_string()
        .contains("source digest is not reconstructible"));

    let mut forged_membership = collapsed.clone();
    let binding = forged_membership
        .metadata_transfer_staging_finalized_floors
        .values_mut()
        .flat_map(|floor| floor.checkpoint_bindings.values_mut())
        .find(|binding| {
            (
                binding.actor_node_id,
                binding.actor_node_incarnation,
                binding.first_generation,
            ) == first_key
        })
        .unwrap();
    binding.page_sequence = binding.page_sequence.checked_add(1).unwrap();
    assert!(parse_snapshot(&format_snapshot(&forged_membership))
        .unwrap_err()
        .to_string()
        .contains("checkpoint anchor"));

    drop(authority);
    let mut restarted = SingleAuthorityControlPlane::open(store).unwrap();
    assert_eq!(
        restarted
            .snapshot()
            .metadata_transfer_staging_evidence_checkpoint_anchors,
        collapsed.metadata_transfer_staging_evidence_checkpoint_anchors
    );
    assert_eq!(
        restarted
            .snapshot()
            .metadata_transfer_staging_finalized_floors,
        collapsed.metadata_transfer_staging_finalized_floors
    );
    let restarted_snapshot = restarted.snapshot().clone();
    let replay = ControlPlaneAdmin::collapse_metadata_transfer_staging_evidence_checkpoint_segment(
        &mut restarted,
        first_key.0,
        first_key.1,
        first_key.2,
        first_last_generation,
    )
    .unwrap();
    assert_eq!(replay, restarted_snapshot);
}

#[test]
fn collapsed_checkpoint_anchor_reconstructs_maximum_commitment_page_from_index() {
    let snapshot = collapsed_staging_checkpoint_snapshot_fixture();
    let ((pg_id, staging_generation), existing_floor) = snapshot
        .metadata_transfer_staging_finalized_floors
        .iter()
        .next()
        .unwrap();
    let publication = existing_floor.publications.first().unwrap();
    let actor = crate::pg_store::MetadataTransferStagingNodeIdentity::new(
        publication.node_id,
        publication.node_incarnation,
        publication.endpoint.clone(),
    )
    .unwrap();
    let transition = snapshot
        .retained_unavailable_pg_placement_transitions
        .get(&(*pg_id, existing_floor.transition.transition_epoch()))
        .unwrap();
    let authorization = transition.staging_authorization.as_ref().unwrap();
    let intent = crate::pg_store::MetadataTransferStagingIntent::for_unavailable_transition(
        &existing_floor.transition,
        authorization.artifact_digest,
        authorization.artifact_length,
        authorization.artifact_format_version,
    )
    .unwrap();

    let mut floor = existing_floor.clone();
    floor.publications.clear();
    floor.tombstones.clear();
    floor.checkpoint_bindings.clear();
    let mut page_entries = Vec::new();
    let mut commitments = BTreeMap::new();
    let mut keys = Vec::new();
    for offset in 1..=MAX_METADATA_TRANSFER_STAGING_EVIDENCE_CHECKPOINT_COMMITMENTS {
        let target_epoch = ClusterEpoch::new(
            existing_floor.transition.transition_epoch().get() + u64::try_from(offset).unwrap(),
        )
        .unwrap();
        let evidence = crate::pg_store::canonical_metadata_transfer_staging_evidence(
            &actor,
            &intent,
            crate::pg_store::MetadataTransferStagingEvidenceKind::Publication,
            Some(target_epoch),
            Some(publication.transfer),
        )
        .unwrap();
        let evidence_digest = checksum::sha256::digest(&evidence);
        let key = MetadataTransferStagingEvidenceKey {
            pg_id: *pg_id,
            staging_generation: *staging_generation,
            actor_node_id: actor.node_id(),
            actor_node_incarnation: actor.node_incarnation(),
            kind: crate::pg_store::MetadataTransferStagingEvidenceKind::Publication,
            target_epoch: Some(target_epoch),
        };
        let sequence = u64::try_from(offset).unwrap();
        floor
            .publications
            .push(MetadataTransferStagingFinalizedPublicationBinding {
                node_id: actor.node_id(),
                node_incarnation: actor.node_incarnation(),
                endpoint: actor.endpoint().to_owned(),
                target_epoch,
                transfer: publication.transfer,
                evidence_digest,
            });
        page_entries.push((sequence, evidence));
        commitments.insert(key.clone(), evidence_digest);
        keys.push((key, sequence));
    }
    let page_digest = crate::pg_store::metadata_transfer_staging_checkpoint_page_digest(
        &actor,
        0,
        [0; 32],
        1,
        &page_entries,
    )
    .unwrap();
    let receipt = crate::pg_store::MetadataTransferStagingEvidenceApplyReceipt::for_checkpoint_link(
        actor.clone(),
        0,
        [0; 32],
        1,
        page_digest,
    );
    let segment = MetadataTransferStagingEvidenceCheckpointSegment {
        actor: actor.clone(),
        first_generation: 1,
        last_generation: 1,
        previous_generation: 0,
        previous_apply_receipt_digest: [0; 32],
        page_links: vec![MetadataTransferStagingEvidenceCheckpointPageLink {
            page_digest,
            previous_apply_receipt_digest: [0; 32],
            apply_receipt_digest: checksum::sha256::digest(receipt.as_bytes()),
            actor_closure_candidate: None,
            entries: keys
                .iter()
                .map(
                    |(key, sequence)| MetadataTransferStagingEvidenceCheckpointPageEntry {
                        sequence: *sequence,
                        evidence_key: key.clone(),
                    },
                )
                .collect(),
        }],
        tip_apply_receipt: receipt.as_bytes().to_vec(),
        commitments,
    };
    let segment_digest = metadata_transfer_staging_checkpoint_segment_digest(&segment);
    for (key, sequence) in keys {
        floor.checkpoint_bindings.insert(
            key,
            MetadataTransferStagingFinalizedCheckpointBinding {
                actor_node_id: actor.node_id(),
                actor_node_incarnation: actor.node_incarnation(),
                actor_endpoint: actor.endpoint().to_owned(),
                first_generation: 1,
                last_generation: 1,
                page_generation: 1,
                page_sequence: sequence,
                segment_digest,
                actor_closure_candidate: None,
            },
        );
    }
    let floors = BTreeMap::from([((*pg_id, *staging_generation), floor)]);
    let finalized_evidence = metadata_transfer_staging_finalized_evidence_index(&floors).unwrap();
    let finalized_checkpoints =
        metadata_transfer_staging_finalized_checkpoint_index(&floors).unwrap();
    let anchor =
        MetadataTransferStagingEvidenceCheckpointAnchor {
            actor,
            first_generation: 1,
            last_generation: 1,
            previous_generation: 0,
            previous_apply_receipt_digest: [0; 32],
            tip_apply_receipt: receipt.as_bytes().to_vec(),
            source_segment_digest: segment_digest,
            source_segment_count: 1,
            source_segments_digest: metadata_transfer_staging_checkpoint_source_segments_digest(&[
                (1, 1, segment_digest),
            ]),
        };

    let reconstructed = snapshot
        .reconstruct_metadata_transfer_staging_checkpoint_source_segment(
            &anchor.actor,
            MetadataTransferStagingCheckpointSourceSegmentBinding {
                first_generation: anchor.first_generation,
                last_generation: anchor.last_generation,
                previous_generation: anchor.previous_generation,
                previous_apply_receipt_digest: anchor.previous_apply_receipt_digest,
                source_segment_digest: anchor.source_segment_digest,
            },
            &finalized_evidence,
            &finalized_checkpoints,
        )
        .unwrap();
    assert_eq!(reconstructed, segment);
    assert_eq!(
        reconstructed.commitments.len(),
        MAX_METADATA_TRANSFER_STAGING_EVIDENCE_CHECKPOINT_COMMITMENTS
    );
}

fn begin_successor_unavailable_transition(
    authority: &mut SingleAuthorityControlPlane<FileControlPlaneStore>,
    pg_id: PgId,
    unavailable_node_id: NodeId,
    proof: PgMetadataProof,
) -> UnavailablePgTransitionMutationBinding {
    let source_acting_set = authority.snapshot().pg(pg_id).unwrap().acting_set.clone();
    assert!(source_acting_set.contains(&unavailable_node_id));
    let heartbeat_at_ms = authority
        .snapshot()
        .max_committed_timestamp_ms()
        .unwrap_or(0)
        + CONTROL_PLANE_CLOCK_SKEW_BUDGET_MS
        + 10;
    for node_id in 1..=4 {
        heartbeat_spare_node(authority, node_id, heartbeat_at_ms + u64::from(node_id));
    }
    for node_id in source_acting_set.iter().copied() {
        heartbeat_with_pg_proof_and_lease_duration(
            authority,
            node_id.as_u32(),
            pg_id.get(),
            PgState::Active,
            proof,
            false,
            (
                heartbeat_at_ms + 10 + u64::from(node_id.as_u32()),
                if node_id == unavailable_node_id {
                    100
                } else {
                    10_000
                },
            ),
        );
    }
    let failed_deadline = authority
        .snapshot()
        .node(unavailable_node_id)
        .unwrap()
        .lease_deadline_ms()
        .unwrap();
    authority.expire_heartbeat_leases(failed_deadline).unwrap();
    for node_id in (1..=4)
        .map(NodeId::new)
        .filter(|node_id| *node_id != unavailable_node_id)
    {
        heartbeat_spare_node(
            authority,
            node_id.as_u32(),
            failed_deadline + 10 + u64::from(node_id.as_u32()),
        );
    }
    for node_id in source_acting_set
        .iter()
        .copied()
        .filter(|node_id| *node_id != unavailable_node_id)
    {
        heartbeat_with_pg_proof_and_lease_duration(
            authority,
            node_id.as_u32(),
            pg_id.get(),
            PgState::Peering,
            proof,
            false,
            (failed_deadline + 20 + u64::from(node_id.as_u32()), 10_000),
        );
    }
    let begin_at_ms = (authority
        .snapshot()
        .unavailable_node_observation(unavailable_node_id)
        .unwrap()
        .observed_at_ms()
        + 50)
        .max(authority.snapshot().max_committed_timestamp_ms().unwrap());
    authority
        .begin_unavailable_pg_placement_transition(pg_id, unavailable_node_id, begin_at_ms)
        .unwrap();
    let transition = authority
        .snapshot()
        .unavailable_pg_placement_transition(pg_id)
        .unwrap();
    UnavailablePgTransitionMutationBinding::new(
        transition.pg_id,
        transition.transition_epoch,
        transition.source_epoch,
        transition.source_acting_set.clone(),
        transition.destination_acting_set.clone(),
    )
}

fn latest_staging_evidence_apply_receipt(
    snapshot: &ClusterControlSnapshot,
    node_id: NodeId,
    node_incarnation: u64,
) -> Option<crate::pg_store::MetadataTransferStagingEvidenceApplyReceipt> {
    snapshot
        .metadata_transfer_staging_evidence_pages
        .range((node_id, node_incarnation, 0)..=(node_id, node_incarnation, u64::MAX))
        .next_back()
        .map(|(_, page)| {
            crate::pg_store::decode_staging_evidence_apply_receipt(&page.apply_receipt).unwrap()
        })
        .or_else(|| {
            snapshot
                .metadata_transfer_staging_evidence_checkpoint_segments
                .values()
                .filter(|segment| {
                    segment.actor.node_id() == node_id
                        && segment.actor.node_incarnation() == node_incarnation
                })
                .max_by_key(|segment| segment.last_generation)
                .map(|segment| {
                    crate::pg_store::decode_staging_evidence_apply_receipt(
                        &segment.tip_apply_receipt,
                    )
                    .unwrap()
                })
        })
}

fn authorize_and_publish_unavailable_transition(
    authority: &mut SingleAuthorityControlPlane<FileControlPlaneStore>,
    binding: &UnavailablePgTransitionMutationBinding,
    proof: PgMetadataProof,
    artifact_byte: u8,
) -> UnavailablePgTransitionInstallRequest {
    let authorization = UnavailablePgStagingIntentAuthorizationRequest {
        unavailable_transition: binding.clone(),
        staging_generation: binding.transition_epoch().get(),
        artifact_target_epoch: next_epoch(authority.snapshot().cluster_epoch()).unwrap(),
        artifact_digest: [artifact_byte; 32],
        artifact_length: 4_096 + u64::from(artifact_byte),
        artifact_format_version: crate::pg_store::METADATA_TRANSFER_STAGED_ARTIFACT_FORMAT_VERSION,
    };
    authority
        .authorize_unavailable_pg_staging_intents_batch(std::slice::from_ref(&authorization))
        .unwrap();
    publish_authorized_unavailable_transition(authority, binding, proof, &authorization)
}

fn publish_authorized_unavailable_transition(
    authority: &mut SingleAuthorityControlPlane<FileControlPlaneStore>,
    binding: &UnavailablePgTransitionMutationBinding,
    proof: PgMetadataProof,
    authorization: &UnavailablePgStagingIntentAuthorizationRequest,
) -> UnavailablePgTransitionInstallRequest {
    let destination_epoch = next_epoch(authority.snapshot().cluster_epoch()).unwrap();
    let transfer = PgMetadataTransferProof::new(binding.source_epoch(), proof);
    let intent = crate::pg_store::MetadataTransferStagingIntent::for_unavailable_transition(
        binding,
        authorization.artifact_digest,
        authorization.artifact_length,
        authorization.artifact_format_version,
    )
    .unwrap();
    for node_id in binding.destination_acting_set().iter().copied() {
        let node = authority.snapshot().node(node_id).unwrap();
        let actor = crate::pg_store::MetadataTransferStagingNodeIdentity::new(
            node_id,
            node.node_incarnation(),
            node.endpoint().to_owned(),
        )
        .unwrap();
        let previous = latest_staging_evidence_apply_receipt(
            authority.snapshot(),
            node_id,
            node.node_incarnation(),
        );
        let page =
            crate::pg_store::metadata_transfer_staging_publication_evidence_page_at_epoch_for_test(
                actor,
                &intent,
                destination_epoch,
                transfer,
                previous.as_ref(),
            );
        ControlPlaneAdmin::apply_metadata_transfer_staging_evidence_page(
            authority,
            page.operation_payload().to_vec(),
            page.page_digest(),
        )
        .unwrap();
    }
    let mut publications = binding
        .destination_acting_set()
        .iter()
        .copied()
        .map(|node_id| {
            let node = authority.snapshot().node(node_id).unwrap();
            let key = MetadataTransferStagingEvidenceKey {
                pg_id: binding.pg_id(),
                staging_generation: authorization.staging_generation,
                actor_node_id: node_id,
                actor_node_incarnation: node.node_incarnation(),
                kind: crate::pg_store::MetadataTransferStagingEvidenceKind::Publication,
                target_epoch: Some(destination_epoch),
            };
            UnavailablePgStagingPublicationBinding {
                node_id,
                node_incarnation: node.node_incarnation(),
                endpoint: node.endpoint().to_owned(),
                evidence_digest: checksum::sha256::digest(
                    &authority.snapshot().metadata_transfer_staging_evidence[&key],
                ),
            }
        })
        .collect::<Vec<_>>();
    publications.sort_by_key(|publication| publication.node_id);
    UnavailablePgTransitionInstallRequest {
        unavailable_transition: binding.clone(),
        transfer,
        expected_destination_epoch: destination_epoch,
        publications,
    }
}

#[test]
fn staging_authorization_retains_prepared_target_across_unrelated_epoch_and_restart() {
    let proof = PgMetadataProof::current(23, 0x2323, 0x3434);
    let (_tmp, store, mut authority, authorizations) =
        begun_two_pg_staging_authorization_authority_fixture_with_proof(proof);
    let prepared = authorizations[0].clone();
    let unrelated_binding = authorizations[1].unavailable_transition.clone();
    let unrelated_install =
        authorize_and_publish_unavailable_transition(&mut authority, &unrelated_binding, proof, 9);
    install_and_complete_unavailable_transition(&mut authority, &unrelated_install);
    assert!(authority.snapshot().cluster_epoch() >= prepared.artifact_target_epoch);

    authority
        .authorize_unavailable_pg_staging_intents_batch(std::slice::from_ref(&prepared))
        .unwrap();
    let authorization_epoch = authority
        .snapshot()
        .unavailable_pg_placement_transition(prepared.unavailable_transition.pg_id())
        .unwrap()
        .staging_authorization
        .as_ref()
        .unwrap()
        .batch_receipt
        .source_epoch;
    assert!(authorization_epoch >= prepared.artifact_target_epoch);

    drop(authority);
    let restarted = SingleAuthorityControlPlane::open(store).unwrap();
    let transition = restarted
        .snapshot()
        .unavailable_pg_placement_transition(prepared.unavailable_transition.pg_id())
        .unwrap();
    let work = UnavailablePgReconciliationWork::from_transition(
        transition,
        UnavailablePgReconciliationStage::MetadataTransfer,
    );
    let (durable, source_epoch, target_epoch) = restarted
        .snapshot()
        .committed_unavailable_pg_staging_request_binding(&work)
        .unwrap();
    assert_eq!(durable, prepared);
    assert_eq!(target_epoch, prepared.artifact_target_epoch);
    assert_eq!(next_epoch(source_epoch).unwrap(), target_epoch);
    assert_ne!(next_epoch(authorization_epoch).unwrap(), target_epoch);
}

fn install_and_complete_unavailable_transition(
    authority: &mut SingleAuthorityControlPlane<FileControlPlaneStore>,
    install: &UnavailablePgTransitionInstallRequest,
) {
    authority
        .install_unavailable_pg_placement_transitions_batch(
            std::slice::from_ref(install),
            install.expected_destination_epoch,
        )
        .unwrap();
    complete_installed_unavailable_transition(authority, install);
}

fn complete_installed_unavailable_transition(
    authority: &mut SingleAuthorityControlPlane<FileControlPlaneStore>,
    install: &UnavailablePgTransitionInstallRequest,
) {
    let heartbeat_at_ms = authority.snapshot().max_committed_timestamp_ms().unwrap()
        + CONTROL_PLANE_CLOCK_SKEW_BUDGET_MS
        + 1;
    for (offset, node_id) in install
        .unavailable_transition
        .destination_acting_set()
        .iter()
        .copied()
        .enumerate()
    {
        heartbeat_with_pg_proof_and_lease_duration(
            authority,
            node_id.as_u32(),
            install.unavailable_transition.pg_id().get(),
            PgState::Peering,
            install.transfer.metadata_proof(),
            false,
            (heartbeat_at_ms + u64::try_from(offset).unwrap(), 10_000),
        );
    }
    let work = UnavailablePgReconciliationWork::from_transition(
        authority
            .snapshot()
            .unavailable_pg_placement_transition(install.unavailable_transition.pg_id())
            .unwrap(),
        UnavailablePgReconciliationStage::PayloadReadiness,
    );
    let ready_at_ms = authority.snapshot().max_committed_timestamp_ms().unwrap() + 1;
    authority
        .complete_unavailable_pg_placement_transition_batch(&[work], ready_at_ms)
        .unwrap();
}

fn append_staging_tombstones(
    authority: &mut SingleAuthorityControlPlane<FileControlPlaneStore>,
    binding: &UnavailablePgTransitionMutationBinding,
) -> (
    FinalizeMetadataTransferStagingGenerationRequest,
    BTreeMap<NodeId, u64>,
) {
    let transition = authority
        .snapshot()
        .retained_unavailable_pg_placement_transitions
        .get(&(binding.pg_id(), binding.transition_epoch()))
        .unwrap()
        .clone();
    let authorization = transition.staging_authorization.as_ref().unwrap();
    let mut page_generations = BTreeMap::new();
    let mut tombstones = Vec::new();
    for node_id in binding.destination_acting_set().iter().copied() {
        let node = authority.snapshot().node(node_id).unwrap();
        let node_incarnation = node.node_incarnation();
        let endpoint = node.endpoint().to_owned();
        let actor = crate::pg_store::MetadataTransferStagingNodeIdentity::new(
            node_id,
            node_incarnation,
            endpoint.clone(),
        )
        .unwrap();
        let previous =
            latest_staging_evidence_apply_receipt(authority.snapshot(), node_id, node_incarnation)
                .unwrap();
        let page = crate::pg_store::metadata_transfer_staging_evidence_page_for_test(
            actor,
            binding,
            authorization.artifact_digest,
            authorization.artifact_length,
            authorization.artifact_format_version,
            crate::pg_store::MetadataTransferStagingEvidenceKind::Tombstone,
            Some(&previous),
        );
        let generation = page.generation();
        ControlPlaneAdmin::apply_metadata_transfer_staging_evidence_page(
            authority,
            page.operation_payload().to_vec(),
            page.page_digest(),
        )
        .unwrap();
        page_generations.insert(node_id, generation);
        let key = MetadataTransferStagingEvidenceKey {
            pg_id: binding.pg_id(),
            staging_generation: authorization.staging_generation,
            actor_node_id: node_id,
            actor_node_incarnation: node_incarnation,
            kind: crate::pg_store::MetadataTransferStagingEvidenceKind::Tombstone,
            target_epoch: None,
        };
        tombstones.push(MetadataTransferStagingTombstoneBinding {
            node_id,
            node_incarnation,
            endpoint,
            evidence_digest: checksum::sha256::digest(
                &authority.snapshot().metadata_transfer_staging_evidence[&key],
            ),
        });
    }
    tombstones.sort_by_key(|tombstone| tombstone.node_id);
    (
        FinalizeMetadataTransferStagingGenerationRequest {
            unavailable_transition: binding.clone(),
            staging_generation: authorization.staging_generation,
            disposition: MetadataTransferStagingCleanupDisposition::Completed,
            tombstones,
        },
        page_generations,
    )
}

#[test]
fn staging_evidence_checkpoint_compacts_history_without_consuming_the_tip() {
    let (_tmp, store, mut authority, installs) = staged_two_pg_install_authority_fixture_with_proof(
        PgMetadataProof::current(23, 0x2323, 0x3434),
    );
    let first_install = &installs[0];
    let actor_node_id = first_install.publications[0].node_id;
    let actor_node_incarnation = first_install.publications[0].node_incarnation;
    let actor_key = (actor_node_id, actor_node_incarnation);
    let original_epoch = authority.snapshot().cluster_epoch();
    let first_page = authority
        .snapshot()
        .metadata_transfer_staging_evidence_pages[&(actor_key.0, actor_key.1, 1)]
        .clone();

    assert!(authority
        .snapshot()
        .apply_control_plane_command(
            ControlPlaneCommand::CheckpointMetadataTransferStagingEvidencePages {
                actor_node_id,
                actor_node_incarnation,
                first_generation: 1,
                last_generation: 65,
            }
        )
        .unwrap_err()
        .to_string()
        .contains("exceeds the 64 page limit"));

    let compacted = ControlPlaneAdmin::checkpoint_metadata_transfer_staging_evidence_pages(
        &mut authority,
        actor_node_id,
        actor_node_incarnation,
        1,
        1,
    )
    .unwrap();
    assert_eq!(compacted.cluster_epoch(), original_epoch);
    assert!(!compacted
        .metadata_transfer_staging_evidence_pages
        .contains_key(&(actor_key.0, actor_key.1, 1)));
    assert!(compacted
        .metadata_transfer_staging_evidence_pages
        .contains_key(&(actor_key.0, actor_key.1, 2)));
    let segment = &compacted.metadata_transfer_staging_evidence_checkpoint_segments
        [&(actor_key.0, actor_key.1, 1)];
    assert_eq!((segment.first_generation, segment.last_generation), (1, 1));
    assert_eq!(segment.commitments.len(), 1);
    compacted.validate_current_state_invariants().unwrap();
    assert_eq!(
        parse_snapshot(&format_snapshot(&compacted)).unwrap(),
        compacted
    );

    let replayed = ControlPlaneAdmin::checkpoint_metadata_transfer_staging_evidence_pages(
        &mut authority,
        actor_node_id,
        actor_node_incarnation,
        1,
        1,
    )
    .unwrap();
    assert_eq!(replayed, compacted);
    assert!(authority
        .snapshot()
        .apply_control_plane_command(
            ControlPlaneCommand::ApplyMetadataTransferStagingEvidencePage {
                operation_payload: first_page.operation_payload,
                page_digest: first_page.page_digest,
            }
        )
        .unwrap_err()
        .to_string()
        .contains("does not extend the retained generation"));

    assert!(
        ControlPlaneAdmin::checkpoint_metadata_transfer_staging_evidence_pages(
            &mut authority,
            actor_node_id,
            actor_node_incarnation,
            2,
            2,
        )
        .unwrap_err()
        .to_string()
        .contains("cannot consume the actor page tip")
    );

    let second_page = &authority
        .snapshot()
        .metadata_transfer_staging_evidence_pages[&(actor_key.0, actor_key.1, 2)];
    let second_receipt =
        crate::pg_store::decode_staging_evidence_apply_receipt(&second_page.apply_receipt).unwrap();
    let transition = first_install.unavailable_transition.clone();
    let actor_record = authority.snapshot().node(actor_node_id).unwrap();
    let actor = crate::pg_store::MetadataTransferStagingNodeIdentity::new(
        actor_node_id,
        actor_node_incarnation,
        actor_record.endpoint().to_owned(),
    )
    .unwrap();
    let authorization = authority
        .snapshot()
        .unavailable_pg_placement_transition(transition.pg_id())
        .unwrap()
        .staging_authorization
        .as_ref()
        .unwrap();
    let intent = crate::pg_store::MetadataTransferStagingIntent::for_unavailable_transition(
        &transition,
        authorization.artifact_digest,
        authorization.artifact_length,
        authorization.artifact_format_version,
    )
    .unwrap();
    let third_page = crate::pg_store::metadata_transfer_staging_evidence_page_for_test(
        actor,
        &transition,
        intent.artifact_digest(),
        intent.artifact_length(),
        intent.artifact_format_version(),
        crate::pg_store::MetadataTransferStagingEvidenceKind::Tombstone,
        Some(&second_receipt),
    );
    ControlPlaneAdmin::apply_metadata_transfer_staging_evidence_page(
        &mut authority,
        third_page.operation_payload().to_vec(),
        third_page.page_digest(),
    )
    .unwrap();
    let third_page = &authority
        .snapshot()
        .metadata_transfer_staging_evidence_pages[&(actor_key.0, actor_key.1, 3)];
    let third_receipt =
        crate::pg_store::decode_staging_evidence_apply_receipt(&third_page.apply_receipt).unwrap();
    let second_install = &installs[1];
    assert!(second_install
        .publications
        .iter()
        .any(|publication| publication.node_id == actor_node_id));
    let second_transition = second_install.unavailable_transition.clone();
    let second_authorization = authority
        .snapshot()
        .unavailable_pg_placement_transition(second_transition.pg_id())
        .unwrap()
        .staging_authorization
        .as_ref()
        .unwrap()
        .clone();
    let fourth_page = crate::pg_store::metadata_transfer_staging_evidence_page_for_test(
        crate::pg_store::MetadataTransferStagingNodeIdentity::new(
            actor_node_id,
            actor_node_incarnation,
            authority
                .snapshot()
                .node(actor_node_id)
                .unwrap()
                .endpoint()
                .to_owned(),
        )
        .unwrap(),
        &second_transition,
        second_authorization.artifact_digest,
        second_authorization.artifact_length,
        second_authorization.artifact_format_version,
        crate::pg_store::MetadataTransferStagingEvidenceKind::Tombstone,
        Some(&third_receipt),
    );
    ControlPlaneAdmin::apply_metadata_transfer_staging_evidence_page(
        &mut authority,
        fourth_page.operation_payload().to_vec(),
        fourth_page.page_digest(),
    )
    .unwrap();
    authority
        .snapshot()
        .validate_current_state_invariants()
        .unwrap();
    let compacted_second = ControlPlaneAdmin::checkpoint_metadata_transfer_staging_evidence_pages(
        &mut authority,
        actor_node_id,
        actor_node_incarnation,
        2,
        3,
    )
    .unwrap();
    assert_eq!(compacted_second.cluster_epoch(), original_epoch);
    let second_segment = &compacted_second.metadata_transfer_staging_evidence_checkpoint_segments
        [&(actor_key.0, actor_key.1, 2)];
    assert_eq!(second_segment.last_generation, 3);
    assert_eq!(second_segment.page_links.len(), 2);
    let formatted_checkpoint_state = format_snapshot(&compacted_second);
    let second_segment_record = formatted_checkpoint_state
        .lines()
        .filter(|line| {
            line.starts_with(METADATA_TRANSFER_STAGING_EVIDENCE_CHECKPOINT_STATE_RECORD_PREFIX)
        })
        .find(|line| line.contains(",2,3,"))
        .unwrap();
    assert_eq!(
        metadata_transfer_staging_evidence_checkpoint_state_record_len(second_segment),
        second_segment_record.len() + 1
    );
    assert!(
        second_segment_record.len()
            < MAX_METADATA_TRANSFER_STAGING_EVIDENCE_CHECKPOINT_STATE_RECORD_BYTES
    );
    assert_eq!(
        compacted_second
            .metadata_transfer_staging_evidence_checkpoint_segments
            .range((actor_key.0, actor_key.1, 0)..=(actor_key.0, actor_key.1, u64::MAX))
            .count(),
        2
    );
    assert!(compacted_second
        .metadata_transfer_staging_evidence_pages
        .contains_key(&(actor_key.0, actor_key.1, 4)));
    compacted_second
        .validate_current_state_invariants()
        .unwrap();
    assert_eq!(
        parse_snapshot(&format_snapshot(&compacted_second)).unwrap(),
        compacted_second
    );
    drop(authority);
    let restarted = SingleAuthorityControlPlane::open(store).unwrap();
    assert_eq!(
        restarted
            .snapshot()
            .metadata_transfer_staging_evidence_checkpoint_segments,
        compacted_second.metadata_transfer_staging_evidence_checkpoint_segments
    );
    assert_eq!(
        restarted
            .snapshot()
            .metadata_transfer_staging_evidence_pages,
        compacted_second.metadata_transfer_staging_evidence_pages
    );
    assert_eq!(
        restarted.snapshot().metadata_transfer_staging_evidence,
        compacted_second.metadata_transfer_staging_evidence
    );
    restarted
        .snapshot()
        .validate_current_state_invariants()
        .unwrap();

    let mut changed_commitment = compacted_second.clone();
    changed_commitment
        .metadata_transfer_staging_evidence_checkpoint_segments
        .get_mut(&(actor_key.0, actor_key.1, 1))
        .unwrap()
        .commitments
        .values_mut()
        .next()
        .unwrap()[0] ^= 1;
    assert!(parse_snapshot(&format_snapshot(&changed_commitment))
        .unwrap_err()
        .to_string()
        .contains("checkpoint commitment digest is invalid"));

    let mut changed_interior_link = compacted_second.clone();
    changed_interior_link
        .metadata_transfer_staging_evidence_checkpoint_segments
        .get_mut(&(actor_key.0, actor_key.1, 2))
        .unwrap()
        .page_links[0]
        .page_digest[0] ^= 1;
    assert!(parse_snapshot(&format_snapshot(&changed_interior_link))
        .unwrap_err()
        .to_string()
        .contains("page membership does not match its digest"));

    let mut changed_commitment_actor = compacted_second.clone();
    changed_commitment_actor
        .nodes
        .get_mut(&actor_node_id)
        .unwrap()
        .node_incarnation += 1;
    let changed_actor = crate::pg_store::MetadataTransferStagingNodeIdentity::new(
        actor_node_id,
        actor_node_incarnation,
        "unix:///changed-historical-actor.sock".to_owned(),
    )
    .unwrap();
    let first_segment = changed_commitment_actor
        .metadata_transfer_staging_evidence_checkpoint_segments
        .get_mut(&(actor_key.0, actor_key.1, 1))
        .unwrap();
    let first_segment_keys = first_segment
        .commitments
        .keys()
        .cloned()
        .collect::<Vec<_>>();
    for key in first_segment_keys {
        let rebound = crate::pg_store::rebind_metadata_transfer_staging_evidence_actor_for_test(
            &changed_commitment_actor.metadata_transfer_staging_evidence[&key],
            &changed_actor,
        );
        first_segment
            .commitments
            .insert(key.clone(), checksum::sha256::digest(&rebound));
        changed_commitment_actor
            .metadata_transfer_staging_evidence
            .insert(key, rebound);
    }
    assert!(parse_snapshot(&format_snapshot(&changed_commitment_actor))
        .unwrap_err()
        .to_string()
        .contains("page membership does not match its digest"));

    let mut changed_page_actor = compacted_second.clone();
    changed_page_actor
        .nodes
        .get_mut(&actor_node_id)
        .unwrap()
        .node_incarnation += 1;
    let changed_actor = crate::pg_store::MetadataTransferStagingNodeIdentity::new(
        actor_node_id,
        actor_node_incarnation,
        "unix:///changed-page-actor.sock".to_owned(),
    )
    .unwrap();
    let changed_fourth_page = crate::pg_store::metadata_transfer_staging_evidence_page_for_test(
        changed_actor,
        &second_transition,
        second_authorization.artifact_digest,
        second_authorization.artifact_length,
        second_authorization.artifact_format_version,
        crate::pg_store::MetadataTransferStagingEvidenceKind::Tombstone,
        Some(&third_receipt),
    );
    let changed_fourth_evidence =
        crate::pg_store::decode_staging_evidence(changed_fourth_page.entries()[0].evidence())
            .unwrap();
    let changed_fourth_key = metadata_transfer_staging_evidence_key(&changed_fourth_evidence);
    changed_page_actor
        .metadata_transfer_staging_evidence
        .insert(
            changed_fourth_key,
            changed_fourth_evidence.as_bytes().to_vec(),
        );
    let changed_fourth_receipt =
        crate::pg_store::MetadataTransferStagingEvidenceApplyReceipt::for_page(
            &changed_fourth_page,
        );
    changed_page_actor
        .metadata_transfer_staging_evidence_pages
        .insert(
            (actor_key.0, actor_key.1, 4),
            MetadataTransferStagingEvidencePageRecord {
                operation_payload: changed_fourth_page.operation_payload().to_vec(),
                page_digest: changed_fourth_page.page_digest(),
                apply_receipt: changed_fourth_receipt.as_bytes().to_vec(),
            },
        );
    let changed_page_actor_error = parse_snapshot(&format_snapshot(&changed_page_actor))
        .unwrap_err()
        .to_string();
    assert!(
        changed_page_actor_error.contains("actor chain has a gap")
            || changed_page_actor_error.contains("must retain a replayable or closed tip"),
        "unexpected changed-page-actor error: {changed_page_actor_error}"
    );

    let mut missing_tip = compacted_second.clone();
    missing_tip
        .metadata_transfer_staging_evidence_pages
        .remove(&(actor_key.0, actor_key.1, 4));
    let error = parse_snapshot(&format_snapshot(&missing_tip))
        .unwrap_err()
        .to_string();
    assert!(
        error.contains("must retain a replayable or closed tip")
            || error.contains("does not exactly match retained chain membership"),
        "unexpected missing-tip error: {error}"
    );
}

#[test]
fn production_staging_maintenance_checkpoints_history_but_retains_open_tips() {
    let (_tmp, _store, mut authority, installs) =
        staged_two_pg_install_authority_fixture_with_proof(PgMetadataProof::current(
            23, 0x2323, 0x3434,
        ));
    let actor_node_id = installs[0].publications[0].node_id;
    let actor_node_incarnation = installs[0].publications[0].node_incarnation;
    let original_epoch = authority.snapshot().cluster_epoch();
    let mut cursor = MetadataTransferStagingMaintenanceCursor::start();

    assert!(authority
        .maintain_metadata_transfer_staging_evidence_once(&mut cursor)
        .unwrap());
    assert_eq!(authority.snapshot().cluster_epoch(), original_epoch);
    assert!(!authority
        .snapshot()
        .metadata_transfer_staging_evidence_pages
        .contains_key(&(actor_node_id, actor_node_incarnation, 1)));
    assert!(authority
        .snapshot()
        .metadata_transfer_staging_evidence_pages
        .contains_key(&(actor_node_id, actor_node_incarnation, 2)));

    for _ in 0..32 {
        authority
            .maintain_metadata_transfer_staging_evidence_once(&mut cursor)
            .unwrap();
    }
    for (node_id, incarnation, generation) in authority
        .snapshot()
        .metadata_transfer_staging_evidence_pages
        .keys()
    {
        let actor_tip = authority
            .snapshot()
            .metadata_transfer_staging_evidence_pages
            .range((*node_id, *incarnation, 0)..=(*node_id, *incarnation, u64::MAX))
            .next_back()
            .unwrap()
            .0
             .2;
        assert_eq!(
            *generation, actor_tip,
            "maintenance consumed an open actor tip"
        );
    }
}

#[test]
fn production_staging_maintenance_retires_closure_before_checkpointing_genesis() {
    let snapshot = staging_actor_closure_snapshot_fixture();
    let closure_key = *snapshot
        .metadata_transfer_staging_actor_closures
        .keys()
        .next()
        .unwrap();
    let destination_actor = snapshot.metadata_transfer_staging_actor_closures[&closure_key]
        .destination_actor
        .clone();
    let tmp = test_util::tempdir();
    let store = FileControlPlaneStore::new(tmp.path().join("control-plane.state"));
    store.checkpoint(None, &snapshot).unwrap();
    let mut authority = SingleAuthorityControlPlane::open(store).unwrap();
    let original_epoch = authority.snapshot().cluster_epoch();
    let mut cursor = MetadataTransferStagingMaintenanceCursor::start();

    assert!(authority
        .maintain_metadata_transfer_staging_evidence_once(&mut cursor)
        .unwrap());
    assert!(authority
        .snapshot()
        .metadata_transfer_staging_retired_actor_closures
        .contains_key(&closure_key));
    assert!(authority
        .snapshot()
        .metadata_transfer_staging_evidence_pages
        .contains_key(&(
            destination_actor.node_id(),
            destination_actor.node_incarnation(),
            1,
        )));

    for _ in 0..32 {
        authority
            .maintain_metadata_transfer_staging_evidence_once(&mut cursor)
            .unwrap();
    }
    assert!(!authority
        .snapshot()
        .metadata_transfer_staging_evidence_pages
        .contains_key(&(
            destination_actor.node_id(),
            destination_actor.node_incarnation(),
            1,
        )));
    assert_eq!(authority.snapshot().cluster_epoch(), original_epoch);
    authority
        .snapshot()
        .validate_current_state_invariants()
        .unwrap();
}

#[test]
fn production_staging_maintenance_collapses_and_coalesces_finalized_history() {
    let (_tmp, _store, mut authority, _, _) =
        finalized_staging_floor_authority_fixture_with_isolated_checkpoints(true);
    let original_epoch = authority.snapshot().cluster_epoch();
    let mut cursor = MetadataTransferStagingMaintenanceCursor::start();
    for _ in 0..96 {
        authority
            .maintain_metadata_transfer_staging_evidence_once(&mut cursor)
            .unwrap();
    }
    assert!(authority
        .snapshot()
        .metadata_transfer_staging_evidence_checkpoint_anchors
        .values()
        .any(|anchor| anchor.source_segment_count == 1));
    assert_eq!(authority.snapshot().cluster_epoch(), original_epoch);
    authority
        .snapshot()
        .validate_current_state_invariants()
        .unwrap();

    let (_tmp, _store, mut authority) = adjacent_collapsed_staging_checkpoint_authority_fixture();
    let original_epoch = authority.snapshot().cluster_epoch();
    let initial_anchor_count = authority
        .snapshot()
        .metadata_transfer_staging_evidence_checkpoint_anchors
        .len();
    let mut cursor = MetadataTransferStagingMaintenanceCursor::start();
    for _ in 0..96 {
        authority
            .maintain_metadata_transfer_staging_evidence_once(&mut cursor)
            .unwrap();
    }
    assert!(authority
        .snapshot()
        .metadata_transfer_staging_evidence_checkpoint_anchors
        .values()
        .any(|anchor| anchor.source_segment_count >= 2));
    assert!(
        authority
            .snapshot()
            .metadata_transfer_staging_evidence_checkpoint_anchors
            .len()
            < initial_anchor_count
    );
    assert_eq!(authority.snapshot().cluster_epoch(), original_epoch);
    authority
        .snapshot()
        .validate_current_state_invariants()
        .unwrap();
}

#[test]
fn production_staging_maintenance_finishes_anchor_sweep_when_coalescing_consumes_ceiling() {
    let (_tmp, _store, mut authority) = adjacent_collapsed_staging_checkpoint_authority_fixture();
    let first_key = *authority
        .snapshot()
        .metadata_transfer_staging_evidence_checkpoint_anchors
        .keys()
        .next()
        .unwrap();
    let second_key = (first_key.0, first_key.1, 2);
    let third_key = (first_key.0, first_key.1, 3);
    assert!(authority
        .snapshot()
        .metadata_transfer_staging_evidence_checkpoint_anchors
        .contains_key(&second_key));
    assert!(authority
        .snapshot()
        .metadata_transfer_staging_evidence_checkpoint_anchors
        .contains_key(&third_key));

    let mut cursor = MetadataTransferStagingMaintenanceCursor::start();
    cursor.next_phase = MetadataTransferStagingMaintenancePhase::AnchorCoalescing;
    cursor.anchor_high_water = Some(second_key);
    assert!(authority
        .maintain_metadata_transfer_staging_evidence_once(&mut cursor)
        .unwrap());
    assert_eq!(cursor.anchor_high_water, None);
    assert_eq!(
        authority
            .snapshot()
            .metadata_transfer_staging_evidence_checkpoint_anchors[&first_key]
            .last_generation,
        2
    );

    // Generation 3 was outside the first sweep's captured ceiling. A fresh sweep must include
    // it and merge it with the replacement anchor at generation 1.
    cursor.next_phase = MetadataTransferStagingMaintenancePhase::AnchorCoalescing;
    assert!(authority
        .maintain_metadata_transfer_staging_evidence_once(&mut cursor)
        .unwrap());
    assert_eq!(
        authority
            .snapshot()
            .metadata_transfer_staging_evidence_checkpoint_anchors[&first_key]
            .last_generation,
        3
    );
    assert!(!authority
        .snapshot()
        .metadata_transfer_staging_evidence_checkpoint_anchors
        .contains_key(&third_key));
    authority
        .snapshot()
        .validate_current_state_invariants()
        .unwrap();
}

fn append_next_epoch_staging_publication(
    authority: &mut SingleAuthorityControlPlane<FileControlPlaneStore>,
    actor: &crate::pg_store::MetadataTransferStagingNodeIdentity,
) {
    let latest = authority
        .snapshot()
        .metadata_transfer_staging_evidence
        .values()
        .filter_map(|bytes| crate::pg_store::decode_staging_evidence(bytes).ok())
        .filter(|evidence| {
            evidence.actor() == actor
                && evidence.kind()
                    == crate::pg_store::MetadataTransferStagingEvidenceKind::Publication
        })
        .max_by_key(|evidence| evidence.target_epoch())
        .expect("maintenance fixture actor must retain publication evidence");
    let target_epoch = next_epoch(
        latest
            .target_epoch()
            .expect("publication evidence must bind a target epoch"),
    )
    .unwrap();
    let previous = latest_staging_evidence_apply_receipt(
        authority.snapshot(),
        actor.node_id(),
        actor.node_incarnation(),
    )
    .expect("maintenance fixture actor must retain its page-chain tip");
    let page =
        crate::pg_store::metadata_transfer_staging_publication_evidence_page_at_epoch_for_test(
            actor.clone(),
            latest.intent(),
            target_epoch,
            latest
                .transfer()
                .expect("publication evidence must retain its transfer proof"),
            Some(&previous),
        );
    ControlPlaneAdmin::apply_metadata_transfer_staging_evidence_page(
        authority,
        page.operation_payload().to_vec(),
        page.page_digest(),
    )
    .unwrap();
}

#[test]
fn production_staging_maintenance_does_not_skip_scanned_records_or_starve_later_phases() {
    let (_tmp, _store, mut authority, actor) = checkpoint_anchor_direct_boundary_fixture();
    let actor_key = (actor.node_id(), actor.node_incarnation());
    assert!(
        authority
            .snapshot()
            .metadata_transfer_staging_evidence_checkpoint_anchors
            .range((actor_key.0, actor_key.1, 0)..=(actor_key.0, actor_key.1, u64::MAX))
            .count()
            > METADATA_TRANSFER_STAGING_MAINTENANCE_SCAN_PAGE_SIZE
    );
    let mut cursor = MetadataTransferStagingMaintenanceCursor::start();

    // First coalesce the beginning of a catalogue larger than one scan page. Keep appending
    // higher evidence generations between polls so the catalogue never needs to wrap before
    // the next low record and the lower-priority phases receive service.
    for _ in 0..8 {
        assert!(authority
            .maintain_metadata_transfer_staging_evidence_once(&mut cursor)
            .unwrap());
        if cursor.next_phase == MetadataTransferStagingMaintenancePhase::ClosureRetirement {
            break;
        }
    }
    assert_eq!(
        cursor.next_phase,
        MetadataTransferStagingMaintenancePhase::ClosureRetirement
    );
    assert!(authority
        .snapshot()
        .metadata_transfer_staging_evidence_checkpoint_anchors
        .contains_key(&(actor_key.0, actor_key.1, 3)));

    append_next_epoch_staging_publication(&mut authority, &actor);
    assert!(authority
        .maintain_metadata_transfer_staging_evidence_once(&mut cursor)
        .unwrap());
    assert_eq!(
        cursor.next_phase,
        MetadataTransferStagingMaintenancePhase::SegmentCollapse
    );

    append_next_epoch_staging_publication(&mut authority, &actor);
    assert!(authority
        .maintain_metadata_transfer_staging_evidence_once(&mut cursor)
        .unwrap());
    assert_eq!(
        cursor.next_phase,
        MetadataTransferStagingMaintenancePhase::AnchorCoalescing
    );

    append_next_epoch_staging_publication(&mut authority, &actor);
    assert!(authority
        .maintain_metadata_transfer_staging_evidence_once(&mut cursor)
        .unwrap());
    assert_eq!(
        cursor.next_phase,
        MetadataTransferStagingMaintenancePhase::ClosureRetirement
    );
    assert!(
        !authority
            .snapshot()
            .metadata_transfer_staging_evidence_checkpoint_anchors
            .contains_key(&(actor_key.0, actor_key.1, 3)),
        "the next low anchor must not be skipped while higher generations are appended"
    );
    authority
        .snapshot()
        .validate_current_state_invariants()
        .unwrap();
}

#[test]
fn production_staging_maintenance_wraps_fixed_page_sweep_before_growing_later_actor() {
    let (_tmp, _store, mut authority, _boundary_actor) =
        checkpoint_anchor_direct_boundary_fixture();
    let mut actors = authority
        .snapshot()
        .metadata_transfer_staging_evidence
        .values()
        .filter_map(|bytes| crate::pg_store::decode_staging_evidence(bytes).ok())
        .map(|evidence| evidence.actor().clone())
        .collect::<Vec<_>>();
    actors.sort_by_key(|actor| (actor.node_id(), actor.node_incarnation()));
    actors.dedup();
    let earlier_actor = actors
        .first()
        .expect("maintenance fixture must retain a first destination actor");
    let later_actor = actors
        .last()
        .expect("maintenance fixture must retain a last destination actor");
    assert_ne!(earlier_actor, later_actor);
    let earlier_tip = authority
        .snapshot()
        .metadata_transfer_staging_evidence_pages
        .range(
            (earlier_actor.node_id(), earlier_actor.node_incarnation(), 0)
                ..=(
                    earlier_actor.node_id(),
                    earlier_actor.node_incarnation(),
                    u64::MAX,
                ),
        )
        .next_back()
        .map(|(key, _)| *key)
        .unwrap();
    let later_tip = authority
        .snapshot()
        .metadata_transfer_staging_evidence_pages
        .range(
            (later_actor.node_id(), later_actor.node_incarnation(), 0)
                ..=(
                    later_actor.node_id(),
                    later_actor.node_incarnation(),
                    u64::MAX,
                ),
        )
        .next_back()
        .map(|(key, _)| *key)
        .unwrap();

    append_next_epoch_staging_publication(&mut authority, later_actor);
    let mut cursor = MetadataTransferStagingMaintenanceCursor::start();
    cursor.next_phase = MetadataTransferStagingMaintenancePhase::PageCheckpoint;
    assert!(authority
        .maintain_metadata_transfer_staging_evidence_once(&mut cursor)
        .unwrap());
    assert!(authority
        .snapshot()
        .metadata_transfer_staging_evidence_pages
        .contains_key(&earlier_tip));
    assert!(!authority
        .snapshot()
        .metadata_transfer_staging_evidence_pages
        .contains_key(&later_tip));

    // A becomes eligible after the sweep passed it. Keep extending B beyond the sweep's fixed
    // high-water mark; B may finish the old sweep, but the next poll must wrap and select A.
    append_next_epoch_staging_publication(&mut authority, earlier_actor);
    append_next_epoch_staging_publication(&mut authority, later_actor);
    cursor.next_phase = MetadataTransferStagingMaintenancePhase::PageCheckpoint;
    assert!(authority
        .maintain_metadata_transfer_staging_evidence_once(&mut cursor)
        .unwrap());
    assert!(authority
        .snapshot()
        .metadata_transfer_staging_evidence_pages
        .contains_key(&earlier_tip));

    append_next_epoch_staging_publication(&mut authority, later_actor);
    cursor.next_phase = MetadataTransferStagingMaintenancePhase::PageCheckpoint;
    assert!(authority
        .maintain_metadata_transfer_staging_evidence_once(&mut cursor)
        .unwrap());
    assert!(
        !authority
            .snapshot()
            .metadata_transfer_staging_evidence_pages
            .contains_key(&earlier_tip),
        "a newly eligible earlier actor must be revisited before the later actor tail drains"
    );
    authority
        .snapshot()
        .validate_current_state_invariants()
        .unwrap();
}

#[test]
fn production_staging_maintenance_wraps_fixed_segment_sweep_to_deferred_earlier_actor() {
    let (_tmp, _store, mut authority, boundary_actor) = checkpoint_anchor_direct_boundary_fixture();
    let mut segment_actors = authority
        .snapshot()
        .metadata_transfer_staging_evidence_checkpoint_segments
        .keys()
        .map(|(node_id, incarnation, _)| (*node_id, *incarnation))
        .collect::<Vec<_>>();
    segment_actors.sort_unstable();
    segment_actors.dedup();
    let [earlier_actor, later_actor] = segment_actors.as_slice() else {
        panic!("maintenance fixture must retain two non-boundary segment actors");
    };
    assert_ne!(
        *earlier_actor,
        (boundary_actor.node_id(), boundary_actor.node_incarnation())
    );

    let (earlier_first, earlier_last) = authority
        .snapshot()
        .metadata_transfer_staging_evidence_checkpoint_segments
        .iter()
        .find(|((node_id, incarnation, _), _)| (*node_id, *incarnation) == *earlier_actor)
        .map(|((_, _, first), segment)| (*first, segment.last_generation))
        .unwrap();
    ControlPlaneAdmin::collapse_metadata_transfer_staging_evidence_checkpoint_segment(
        &mut authority,
        earlier_actor.0,
        earlier_actor.1,
        earlier_first,
        earlier_last,
    )
    .unwrap();

    let earlier_identity = authority
        .snapshot()
        .metadata_transfer_staging_evidence
        .values()
        .filter_map(|bytes| crate::pg_store::decode_staging_evidence(bytes).ok())
        .map(|evidence| evidence.actor().clone())
        .find(|actor| (actor.node_id(), actor.node_incarnation()) == *earlier_actor)
        .unwrap();
    let earlier_open_tip = authority
        .snapshot()
        .metadata_transfer_staging_evidence_pages
        .range((earlier_actor.0, earlier_actor.1, 0)..=(earlier_actor.0, earlier_actor.1, u64::MAX))
        .next_back()
        .map(|(key, _)| *key)
        .unwrap();
    append_next_epoch_staging_publication(&mut authority, &earlier_identity);
    ControlPlaneAdmin::checkpoint_metadata_transfer_staging_evidence_pages(
        &mut authority,
        earlier_open_tip.0,
        earlier_open_tip.1,
        earlier_open_tip.2,
        earlier_open_tip.2,
    )
    .unwrap();
    let deferred_segment_key = earlier_open_tip;
    let later_segment_key = authority
        .snapshot()
        .metadata_transfer_staging_evidence_checkpoint_segments
        .keys()
        .find(|(node_id, incarnation, _)| (*node_id, *incarnation) == *later_actor)
        .copied()
        .unwrap();

    let mut cursor = MetadataTransferStagingMaintenanceCursor::start();
    cursor.next_phase = MetadataTransferStagingMaintenancePhase::SegmentCollapse;
    assert!(authority
        .maintain_metadata_transfer_staging_evidence_once(&mut cursor)
        .unwrap());
    assert!(authority
        .snapshot()
        .metadata_transfer_staging_evidence_checkpoint_segments
        .contains_key(&deferred_segment_key));
    assert!(!authority
        .snapshot()
        .metadata_transfer_staging_evidence_checkpoint_segments
        .contains_key(&later_segment_key));
    assert_eq!(cursor.after_segment, Some(later_segment_key));
    assert_eq!(cursor.segment_high_water, Some(later_segment_key));

    cursor.next_phase = MetadataTransferStagingMaintenancePhase::SegmentCollapse;
    authority
        .maintain_metadata_transfer_staging_evidence_once(&mut cursor)
        .unwrap();
    assert_eq!(cursor.after_segment, Some(deferred_segment_key));
    assert_eq!(cursor.segment_high_water, Some(deferred_segment_key));
    assert!(authority
        .snapshot()
        .metadata_transfer_staging_evidence_checkpoint_segments
        .contains_key(&deferred_segment_key));
    authority
        .snapshot()
        .validate_current_state_invariants()
        .unwrap();
}

#[test]
fn finalized_staging_floor_retains_raw_tip_until_later_checkpoint_and_is_epoch_neutral() {
    let (_tmp, store, mut authority, installs) = completed_staged_two_pg_authority_fixture();
    let original_epoch = authority.snapshot().cluster_epoch();
    let first_binding = installs[0].unavailable_transition.clone();
    let second_binding = installs[1].unavailable_transition.clone();
    let first_transition = authority
        .snapshot()
        .retained_unavailable_pg_placement_transitions
        .get(&(first_binding.pg_id(), first_binding.transition_epoch()))
        .unwrap()
        .clone();
    let second_transition = authority
        .snapshot()
        .retained_unavailable_pg_placement_transitions
        .get(&(second_binding.pg_id(), second_binding.transition_epoch()))
        .unwrap()
        .clone();
    let first_authorization = first_transition.staging_authorization.as_ref().unwrap();
    let second_authorization = second_transition.staging_authorization.as_ref().unwrap();

    for node_id in first_transition.destination_acting_set.iter().copied() {
        let node = authority.snapshot().node(node_id).unwrap();
        let actor = crate::pg_store::MetadataTransferStagingNodeIdentity::new(
            node_id,
            node.node_incarnation(),
            node.endpoint().to_owned(),
        )
        .unwrap();
        let current_page = authority
            .snapshot()
            .metadata_transfer_staging_evidence_pages
            .range(
                (node_id, actor.node_incarnation(), 0)
                    ..=(node_id, actor.node_incarnation(), u64::MAX),
            )
            .next_back()
            .unwrap()
            .1;
        let current_receipt =
            crate::pg_store::decode_staging_evidence_apply_receipt(&current_page.apply_receipt)
                .unwrap();
        let first_tombstone = crate::pg_store::metadata_transfer_staging_evidence_page_for_test(
            actor.clone(),
            &first_binding,
            first_authorization.artifact_digest,
            first_authorization.artifact_length,
            first_authorization.artifact_format_version,
            crate::pg_store::MetadataTransferStagingEvidenceKind::Tombstone,
            Some(&current_receipt),
        );
        let first_receipt = ControlPlaneAdmin::apply_metadata_transfer_staging_evidence_page(
            &mut authority,
            first_tombstone.operation_payload().to_vec(),
            first_tombstone.page_digest(),
        )
        .unwrap();
        let first_receipt =
            crate::pg_store::decode_staging_evidence_apply_receipt(&first_receipt).unwrap();
        let second_tombstone = crate::pg_store::metadata_transfer_staging_evidence_page_for_test(
            actor,
            &second_binding,
            second_authorization.artifact_digest,
            second_authorization.artifact_length,
            second_authorization.artifact_format_version,
            crate::pg_store::MetadataTransferStagingEvidenceKind::Tombstone,
            Some(&first_receipt),
        );
        ControlPlaneAdmin::apply_metadata_transfer_staging_evidence_page(
            &mut authority,
            second_tombstone.operation_payload().to_vec(),
            second_tombstone.page_digest(),
        )
        .unwrap();
    }

    let mut tombstones = first_transition
        .destination_acting_set
        .iter()
        .copied()
        .map(|node_id| {
            let node = authority.snapshot().node(node_id).unwrap();
            let key = MetadataTransferStagingEvidenceKey {
                pg_id: first_binding.pg_id(),
                staging_generation: first_authorization.staging_generation,
                actor_node_id: node_id,
                actor_node_incarnation: node.node_incarnation(),
                kind: crate::pg_store::MetadataTransferStagingEvidenceKind::Tombstone,
                target_epoch: None,
            };
            MetadataTransferStagingTombstoneBinding {
                node_id,
                node_incarnation: node.node_incarnation(),
                endpoint: node.endpoint().to_owned(),
                evidence_digest: checksum::sha256::digest(
                    &authority.snapshot().metadata_transfer_staging_evidence[&key],
                ),
            }
        })
        .collect::<Vec<_>>();
    tombstones.sort_by_key(|tombstone| tombstone.node_id);
    let cleanup = FinalizeMetadataTransferStagingGenerationRequest {
        unavailable_transition: first_binding.clone(),
        staging_generation: first_authorization.staging_generation,
        disposition: MetadataTransferStagingCleanupDisposition::Completed,
        tombstones,
    };
    let mut partial = cleanup.clone();
    partial.tombstones.pop();
    assert!(authority
        .snapshot()
        .apply_control_plane_command(
            ControlPlaneCommand::FinalizeMetadataTransferStagingGeneration { cleanup: partial }
        )
        .unwrap_err()
        .to_string()
        .contains("does not match its authorization obligations"));
    let finalized = ControlPlaneAdmin::finalize_metadata_transfer_staging_generation(
        &mut authority,
        cleanup.clone(),
    )
    .unwrap();
    assert_eq!(finalized.cluster_epoch(), original_epoch);
    assert_eq!(
        finalized.metadata_transfer_staging_finalized_floors[&(
            first_binding.pg_id(),
            first_authorization.staging_generation
        )]
            .staging_generation,
        first_authorization.staging_generation
    );
    assert!(finalized
        .metadata_transfer_staging_evidence
        .keys()
        .any(|key| {
            key.pg_id == first_binding.pg_id()
                && key.staging_generation == first_authorization.staging_generation
        }));
    assert!(finalized
        .metadata_transfer_staging_evidence
        .keys()
        .any(|key| {
            key.pg_id == second_binding.pg_id()
                && key.staging_generation == second_authorization.staging_generation
        }));
    finalized.validate_current_state_invariants().unwrap();
    assert_eq!(
        parse_snapshot(&format_snapshot(&finalized)).unwrap(),
        finalized
    );
    let retained_key = finalized
        .metadata_transfer_staging_evidence
        .keys()
        .find(|key| {
            key.pg_id == first_binding.pg_id()
                && key.staging_generation == first_authorization.staging_generation
        })
        .cloned()
        .unwrap();
    let mut missing_detail = finalized.clone();
    missing_detail
        .metadata_transfer_staging_evidence
        .remove(&retained_key);
    assert!(parse_snapshot(&format_snapshot(&missing_detail))
        .unwrap_err()
        .to_string()
        .contains("neither detail nor checkpoint binding"));
    let mut corrupted_detail = finalized.clone();
    corrupted_detail
        .metadata_transfer_staging_evidence
        .get_mut(&retained_key)
        .unwrap()
        .push(0);
    assert!(parse_snapshot(&format_snapshot(&corrupted_detail)).is_err());

    let replay = ControlPlaneAdmin::finalize_metadata_transfer_staging_generation(
        &mut authority,
        cleanup.clone(),
    )
    .unwrap();
    assert_eq!(replay, finalized);

    for node_id in first_transition.destination_acting_set.iter().copied() {
        let incarnation = authority
            .snapshot()
            .node(node_id)
            .unwrap()
            .node_incarnation();
        ControlPlaneAdmin::checkpoint_metadata_transfer_staging_evidence_pages(
            &mut authority,
            node_id,
            incarnation,
            1,
            3,
        )
        .unwrap();
    }
    let checkpointed = authority.snapshot().clone();
    assert!(!checkpointed
        .metadata_transfer_staging_evidence
        .keys()
        .any(|key| {
            key.pg_id == first_binding.pg_id()
                && key.staging_generation == first_authorization.staging_generation
        }));
    let floor = &checkpointed.metadata_transfer_staging_finalized_floors[&(
        first_binding.pg_id(),
        first_authorization.staging_generation,
    )];
    assert_eq!(
        floor.checkpoint_bindings.len(),
        floor.publications.len() + floor.tombstones.len()
    );
    checkpointed.validate_current_state_invariants().unwrap();
    assert_eq!(
        parse_snapshot(&format_snapshot(&checkpointed)).unwrap(),
        checkpointed
    );
    assert_eq!(
        ControlPlaneAdmin::finalize_metadata_transfer_staging_generation(
            &mut authority,
            cleanup.clone(),
        )
        .unwrap(),
        checkpointed
    );

    let stale_actor = &cleanup.tombstones[0];
    let stale_page_tip = authority
        .snapshot()
        .metadata_transfer_staging_evidence_pages
        .range(
            (stale_actor.node_id, stale_actor.node_incarnation, 0)
                ..=(stale_actor.node_id, stale_actor.node_incarnation, u64::MAX),
        )
        .next_back()
        .unwrap()
        .1;
    let stale_predecessor =
        crate::pg_store::decode_staging_evidence_apply_receipt(&stale_page_tip.apply_receipt)
            .unwrap();
    let stale_page = crate::pg_store::metadata_transfer_staging_evidence_page_for_test(
        crate::pg_store::MetadataTransferStagingNodeIdentity::new(
            stale_actor.node_id,
            stale_actor.node_incarnation,
            stale_actor.endpoint.clone(),
        )
        .unwrap(),
        &first_binding,
        first_authorization.artifact_digest,
        first_authorization.artifact_length,
        first_authorization.artifact_format_version,
        crate::pg_store::MetadataTransferStagingEvidenceKind::Tombstone,
        Some(&stale_predecessor),
    );
    assert!(authority
        .snapshot()
        .apply_control_plane_command(
            ControlPlaneCommand::ApplyMetadataTransferStagingEvidencePage {
                operation_payload: stale_page.operation_payload().to_vec(),
                page_digest: stale_page.page_digest(),
            }
        )
        .unwrap_err()
        .to_string()
        .contains("at or below finalized floor"));
    assert_eq!(authority.snapshot(), &checkpointed);

    let first_actor = &cleanup.tombstones[0];
    let checkpoint = checkpointed
        .metadata_transfer_staging_evidence_checkpoint_segments
        .get(&(first_actor.node_id, first_actor.node_incarnation, 1))
        .unwrap();
    assert!(checkpoint.commitments.keys().any(|key| {
        key.pg_id == first_binding.pg_id()
            && key.staging_generation == first_authorization.staging_generation
    }));
    drop(authority);
    let restarted = SingleAuthorityControlPlane::open(store).unwrap();
    assert_eq!(
        restarted
            .snapshot()
            .metadata_transfer_staging_finalized_floors,
        checkpointed.metadata_transfer_staging_finalized_floors
    );
    assert_eq!(
        restarted.snapshot().metadata_transfer_staging_evidence,
        checkpointed.metadata_transfer_staging_evidence
    );
    restarted
        .snapshot()
        .validate_current_state_invariants()
        .unwrap();

    let mut forged = checkpointed.clone();
    forged
        .metadata_transfer_staging_finalized_floors
        .get_mut(&(
            first_binding.pg_id(),
            first_authorization.staging_generation,
        ))
        .unwrap()
        .tombstone_set_digest[0] ^= 1;
    assert!(parse_snapshot(&format_snapshot(&forged))
        .unwrap_err()
        .to_string()
        .contains("invalid identity or digest"));

    let mut forged_checkpoint_binding = checkpointed.clone();
    let floor = forged_checkpoint_binding
        .metadata_transfer_staging_finalized_floors
        .get_mut(&(
            first_binding.pg_id(),
            first_authorization.staging_generation,
        ))
        .unwrap();
    floor.tombstones[0].evidence_digest[0] ^= 1;
    floor.tombstone_set_digest = metadata_transfer_staging_cleanup_digest(
        &floor.transition,
        floor.staging_generation,
        floor.disposition,
        floor.artifact_digest,
        floor.artifact_length,
        floor.artifact_format_version,
        &floor.tombstones,
    );
    let forged_checkpoint_error =
        parse_snapshot(&format_snapshot(&forged_checkpoint_binding)).unwrap_err();
    assert!(
        forged_checkpoint_error
            .to_string()
            .contains("does not match its finalized certificate"),
        "unexpected forged checkpoint error: {forged_checkpoint_error}"
    );

    let mut coordinated_forgery = checkpointed.clone();
    let forged_tombstone = coordinated_forgery
        .metadata_transfer_staging_finalized_floors
        .get_mut(&(
            first_binding.pg_id(),
            first_authorization.staging_generation,
        ))
        .unwrap()
        .tombstones
        .first_mut()
        .unwrap();
    let forged_key = MetadataTransferStagingEvidenceKey {
        pg_id: first_binding.pg_id(),
        staging_generation: first_authorization.staging_generation,
        actor_node_id: forged_tombstone.node_id,
        actor_node_incarnation: forged_tombstone.node_incarnation,
        kind: crate::pg_store::MetadataTransferStagingEvidenceKind::Tombstone,
        target_epoch: None,
    };
    forged_tombstone.evidence_digest[0] ^= 1;
    let forged_digest = forged_tombstone.evidence_digest;
    let floor = coordinated_forgery
        .metadata_transfer_staging_finalized_floors
        .get_mut(&(
            first_binding.pg_id(),
            first_authorization.staging_generation,
        ))
        .unwrap();
    floor.tombstone_set_digest = metadata_transfer_staging_cleanup_digest(
        &floor.transition,
        floor.staging_generation,
        floor.disposition,
        floor.artifact_digest,
        floor.artifact_length,
        floor.artifact_format_version,
        &floor.tombstones,
    );
    *coordinated_forgery
        .metadata_transfer_staging_evidence_checkpoint_segments
        .values_mut()
        .find_map(|segment| segment.commitments.get_mut(&forged_key))
        .unwrap() = forged_digest;
    let coordinated_error = parse_snapshot(&format_snapshot(&coordinated_forgery)).unwrap_err();
    assert!(
        coordinated_error
            .to_string()
            .contains("not canonically committed"),
        "unexpected coordinated checkpoint forgery error: {coordinated_error}"
    );

    let mut membership_forgery = checkpointed.clone();
    let entry = membership_forgery
        .metadata_transfer_staging_evidence_checkpoint_segments
        .values_mut()
        .find_map(|segment| {
            segment
                .page_links
                .iter_mut()
                .flat_map(|link| &mut link.entries)
                .find(|entry| entry.evidence_key == forged_key)
        })
        .unwrap();
    entry.sequence = entry.sequence.checked_add(1).unwrap();
    let membership_error = parse_snapshot(&format_snapshot(&membership_forgery)).unwrap_err();
    assert!(
        membership_error
            .to_string()
            .contains("page membership does not match its digest"),
        "unexpected checkpoint membership forgery error: {membership_error}"
    );
}

#[test]
fn actor_rollover_replays_complete_finalized_prefix_without_restoring_detail() {
    let (_tmp, _store, authority, _installs, cleanup) = finalized_staging_floor_authority_fixture();
    let finalized = authority.snapshot().clone();
    let source_binding = cleanup.tombstones.first().unwrap();
    let source_key = (source_binding.node_id, source_binding.node_incarnation);
    let finalized_evidence = metadata_transfer_staging_finalized_evidence_index(
        &finalized.metadata_transfer_staging_finalized_floors,
    )
    .unwrap();
    let finalized_checkpoints = metadata_transfer_staging_finalized_checkpoint_index(
        &finalized.metadata_transfer_staging_finalized_floors,
    )
    .unwrap();
    let closure_index = finalized
        .metadata_transfer_staging_actor_closure_validation_index(
            &finalized_evidence,
            &finalized_checkpoints,
        )
        .unwrap();
    let source_tip = closure_index.actor_tips.get(&source_key).unwrap().clone();
    let source_entries = closure_index
        .actor_entries
        .get(&source_key)
        .unwrap()
        .iter()
        .map(|(sequence, evidence)| (*sequence, evidence.clone()))
        .collect::<Vec<_>>();
    assert!(source_entries.len() > 1);
    drop(closure_index);
    drop(finalized_checkpoints);
    drop(finalized_evidence);

    let destination_actor = crate::pg_store::MetadataTransferStagingNodeIdentity::new(
        source_binding.node_id,
        source_binding.node_incarnation.checked_add(1).unwrap(),
        format!("{}-restarted", source_binding.endpoint),
    )
    .unwrap();
    let heartbeat_at_ms = finalized.max_committed_timestamp_ms().unwrap_or(0).max(
        finalized
            .node(source_binding.node_id)
            .and_then(NodeControlRecord::lease_deadline_ms)
            .unwrap_or(0),
    ) + 1;
    let mut heartbeat = heartbeat_from_snapshot(
        &finalized,
        source_binding.node_id.as_u32(),
        finalized.cluster_epoch(),
        heartbeat_at_ms,
    );
    heartbeat.node_incarnation = destination_actor.node_incarnation();
    heartbeat.endpoint = destination_actor.endpoint().to_owned();
    heartbeat.requested_lease_duration_ms = 10_000;
    let actor_advanced = finalized
        .apply_control_plane_command(ControlPlaneCommand::RecordNodeHeartbeat {
            heartbeat,
            heartbeat_at_ms,
            lease_deadline_ms: heartbeat_at_ms + 10_000,
            lease_horizon_authority: None,
        })
        .unwrap()
        .into_snapshot();

    let all_sequences = source_entries
        .iter()
        .map(|(sequence, _)| *sequence)
        .collect::<Vec<_>>();
    let complete_page =
        crate::pg_store::metadata_transfer_staging_closure_page_from_entries_for_test(
            source_tip.0.clone(),
            source_tip.1,
            source_tip.3,
            source_tip.0.clone(),
            destination_actor.clone(),
            &source_entries,
            &all_sequences,
        );
    let complete = actor_advanced
        .apply_control_plane_command(
            ControlPlaneCommand::ApplyMetadataTransferStagingEvidencePage {
                operation_payload: complete_page.operation_payload().to_vec(),
                page_digest: complete_page.page_digest(),
            },
        )
        .unwrap()
        .into_snapshot();
    assert!(complete
        .metadata_transfer_staging_actor_closures
        .contains_key(&source_key));
    assert!(!complete
        .metadata_transfer_staging_evidence
        .keys()
        .any(|key| {
            key.pg_id == cleanup.unavailable_transition.pg_id()
                && key.staging_generation == cleanup.staging_generation
        }));
    complete.validate_current_state_invariants().unwrap();

    let omitted_finalized_sequence = source_entries
        .iter()
        .find_map(|(sequence, bytes)| {
            let evidence = crate::pg_store::decode_staging_evidence(bytes).unwrap();
            (evidence.intent().pg_id() == cleanup.unavailable_transition.pg_id()
                && evidence.intent().staging_generation() == cleanup.staging_generation
                && Some(*sequence) != all_sequences.last().copied())
            .then_some(*sequence)
        })
        .expect("the finalized prefix has a nonterminal member to omit");
    let omitted_sequences = all_sequences
        .iter()
        .copied()
        .filter(|sequence| *sequence != omitted_finalized_sequence)
        .collect::<Vec<_>>();
    assert_eq!(
        omitted_sequences.last(),
        all_sequences.last(),
        "the incomplete prefix must reach the committed maximum sequence"
    );
    let omitted_page =
        crate::pg_store::metadata_transfer_staging_closure_page_from_entries_for_test(
            source_tip.0.clone(),
            source_tip.1,
            source_tip.3,
            source_tip.0,
            destination_actor,
            &source_entries,
            &omitted_sequences,
        );
    let unchanged = actor_advanced.clone();
    let error = actor_advanced
        .apply_control_plane_command(
            ControlPlaneCommand::ApplyMetadataTransferStagingEvidencePage {
                operation_payload: omitted_page.operation_payload().to_vec(),
                page_digest: omitted_page.page_digest(),
            },
        )
        .unwrap_err();
    assert!(
        error
            .to_string()
            .contains("cannot complete its committed prefix"),
        "unexpected incomplete finalized-prefix error: {error}"
    );
    assert_eq!(actor_advanced, unchanged);
}

#[test]
fn checkpointed_rollover_genesis_authorizes_later_finalized_page() {
    let (_tmp, _store, authority, source_actor) = checkpoint_anchor_direct_boundary_fixture();
    let finalized = authority.snapshot().clone();
    let source_key = (source_actor.node_id(), source_actor.node_incarnation());
    let finalized_evidence = metadata_transfer_staging_finalized_evidence_index(
        &finalized.metadata_transfer_staging_finalized_floors,
    )
    .unwrap();
    let finalized_checkpoints = metadata_transfer_staging_finalized_checkpoint_index(
        &finalized.metadata_transfer_staging_finalized_floors,
    )
    .unwrap();
    let closure_index = finalized
        .metadata_transfer_staging_actor_closure_validation_index(
            &finalized_evidence,
            &finalized_checkpoints,
        )
        .unwrap();
    let source_tip = closure_index.actor_tips.get(&source_key).unwrap().clone();
    let source_entries = closure_index
        .actor_entries
        .get(&source_key)
        .unwrap()
        .iter()
        .map(|(sequence, evidence)| (*sequence, evidence.clone()))
        .collect::<Vec<_>>();
    assert_eq!(
        source_entries.len(),
        crate::pg_store::MAX_STAGING_EPOCH_PROOFS_PER_INTENT + 2
    );
    let finalized_entry_count = source_entries
        .iter()
        .filter(|(_, bytes)| {
            let evidence = crate::pg_store::decode_staging_evidence(bytes).unwrap();
            metadata_transfer_staging_finalized_generation(
                &finalized.metadata_transfer_staging_finalized_floors,
                evidence.intent().pg_id(),
            )
            .is_some_and(|floor| evidence.intent().staging_generation() <= floor)
        })
        .count();
    assert_eq!(
        finalized_entry_count,
        crate::pg_store::MAX_STAGING_EPOCH_PROOFS_PER_INTENT + 1
    );
    drop(closure_index);
    drop(finalized_checkpoints);
    drop(finalized_evidence);

    let destination_actor = crate::pg_store::MetadataTransferStagingNodeIdentity::new(
        source_actor.node_id(),
        source_actor.node_incarnation().checked_add(1).unwrap(),
        format!("{}-restarted", source_actor.endpoint()),
    )
    .unwrap();
    let heartbeat_at_ms = finalized.max_committed_timestamp_ms().unwrap_or(0).max(
        finalized
            .node(source_actor.node_id())
            .and_then(NodeControlRecord::lease_deadline_ms)
            .unwrap_or(0),
    ) + 1;
    let mut heartbeat = heartbeat_from_snapshot(
        &finalized,
        source_actor.node_id().as_u32(),
        finalized.cluster_epoch(),
        heartbeat_at_ms,
    );
    heartbeat.node_incarnation = destination_actor.node_incarnation();
    heartbeat.endpoint = destination_actor.endpoint().to_owned();
    heartbeat.requested_lease_duration_ms = 10_000;
    let actor_advanced = finalized
        .apply_control_plane_command(ControlPlaneCommand::RecordNodeHeartbeat {
            heartbeat,
            heartbeat_at_ms,
            lease_deadline_ms: heartbeat_at_ms + 10_000,
            lease_horizon_authority: None,
        })
        .unwrap()
        .into_snapshot();

    let first_page_sequences = source_entries
        .iter()
        .take(crate::pg_store::MAX_STAGING_EVIDENCE_PAGE_ENTRIES)
        .map(|(sequence, _)| *sequence)
        .collect::<Vec<_>>();
    assert_eq!(
        first_page_sequences.len(),
        crate::pg_store::MAX_STAGING_EVIDENCE_PAGE_ENTRIES
    );
    let first_page = crate::pg_store::metadata_transfer_staging_closure_page_from_entries_for_test(
        source_tip.0.clone(),
        source_tip.1,
        source_tip.3,
        source_tip.0.clone(),
        destination_actor.clone(),
        &source_entries,
        &first_page_sequences,
    );
    let first_applied = actor_advanced
        .apply_control_plane_command(
            ControlPlaneCommand::ApplyMetadataTransferStagingEvidencePage {
                operation_payload: first_page.operation_payload().to_vec(),
                page_digest: first_page.page_digest(),
            },
        )
        .unwrap()
        .into_snapshot();
    assert!(!first_applied
        .metadata_transfer_staging_actor_closures
        .contains_key(&source_key));

    let first_receipt =
        crate::pg_store::MetadataTransferStagingEvidenceApplyReceipt::for_page(&first_page);
    let second_page =
        crate::pg_store::metadata_transfer_staging_successor_page_from_entries_for_test(
            &source_actor,
            &destination_actor,
            &source_entries[crate::pg_store::MAX_STAGING_EVIDENCE_PAGE_ENTRIES..],
            &first_receipt,
        );
    assert_eq!(second_page.generation(), 2);
    assert!(second_page.entries().iter().any(|entry| {
        let evidence = crate::pg_store::decode_staging_evidence(entry.evidence()).unwrap();
        metadata_transfer_staging_finalized_generation(
            &first_applied.metadata_transfer_staging_finalized_floors,
            evidence.intent().pg_id(),
        )
        .is_some_and(|floor| evidence.intent().staging_generation() <= floor)
    }));
    let complete = first_applied
        .apply_control_plane_command(
            ControlPlaneCommand::ApplyMetadataTransferStagingEvidencePage {
                operation_payload: second_page.operation_payload().to_vec(),
                page_digest: second_page.page_digest(),
            },
        )
        .unwrap()
        .into_snapshot();
    let retirement = complete
        .retire_metadata_transfer_staging_actor_closure_command(source_key.0, source_key.1)
        .unwrap();
    let retired = complete
        .apply_control_plane_command(retirement)
        .unwrap()
        .into_snapshot();
    let checkpointed = retired
        .checkpoint_metadata_transfer_staging_evidence_pages(
            destination_actor.node_id(),
            destination_actor.node_incarnation(),
            1,
            1,
        )
        .unwrap()
        .into_snapshot();
    assert!(!checkpointed
        .metadata_transfer_staging_evidence_pages
        .contains_key(&(
            destination_actor.node_id(),
            destination_actor.node_incarnation(),
            1,
        )));
    assert!(checkpointed
        .metadata_transfer_staging_evidence_pages
        .contains_key(&(
            destination_actor.node_id(),
            destination_actor.node_incarnation(),
            2,
        )));
    checkpointed.validate_current_state_invariants().unwrap();
}

#[test]
fn same_pg_staging_generations_finalize_cumulatively_and_replay_exactly() {
    let (_tmp, store, mut authority, installs, first_cleanup) =
        finalized_staging_floor_authority_fixture();
    let pg_id = first_cleanup.unavailable_transition.pg_id();
    let proof = installs[0].transfer.metadata_proof();

    let second_binding =
        begin_successor_unavailable_transition(&mut authority, pg_id, NodeId::new(4), proof);
    let second_install =
        authorize_and_publish_unavailable_transition(&mut authority, &second_binding, proof, 0x91);
    install_and_complete_unavailable_transition(&mut authority, &second_install);
    let (second_cleanup, second_tombstone_pages) =
        append_staging_tombstones(&mut authority, &second_binding);

    // A later transition supplies a successor page for actors 2 and 3, but
    // excludes actor 1. A further transition supplies actor 1's successor.
    // This preserves every accepted page tip while checkpointing generation 2.
    let third_binding =
        begin_successor_unavailable_transition(&mut authority, pg_id, NodeId::new(1), proof);
    let third_install =
        authorize_and_publish_unavailable_transition(&mut authority, &third_binding, proof, 0x92);
    install_and_complete_unavailable_transition(&mut authority, &third_install);
    let fourth_binding =
        begin_successor_unavailable_transition(&mut authority, pg_id, NodeId::new(2), proof);
    authorize_and_publish_unavailable_transition(&mut authority, &fourth_binding, proof, 0x93);

    for (node_id, tombstone_page_generation) in second_tombstone_pages {
        let node_incarnation = authority
            .snapshot()
            .node(node_id)
            .unwrap()
            .node_incarnation();
        let first_retained_generation = authority
            .snapshot()
            .metadata_transfer_staging_evidence_pages
            .range((node_id, node_incarnation, 0)..=(node_id, node_incarnation, u64::MAX))
            .next()
            .unwrap()
            .0
             .2;
        let current_tip = authority
            .snapshot()
            .metadata_transfer_staging_evidence_pages
            .range((node_id, node_incarnation, 0)..=(node_id, node_incarnation, u64::MAX))
            .next_back()
            .unwrap()
            .0
             .2;
        assert!(current_tip > tombstone_page_generation);
        ControlPlaneAdmin::checkpoint_metadata_transfer_staging_evidence_pages(
            &mut authority,
            node_id,
            node_incarnation,
            first_retained_generation,
            tombstone_page_generation,
        )
        .unwrap();
    }

    let finalized = ControlPlaneAdmin::finalize_metadata_transfer_staging_generation(
        &mut authority,
        second_cleanup.clone(),
    )
    .unwrap();
    let first_generation = first_cleanup.staging_generation;
    let second_generation = second_cleanup.staging_generation;
    assert!(second_generation > first_generation);
    assert!(finalized
        .metadata_transfer_staging_finalized_floors
        .contains_key(&(pg_id, first_generation)));
    assert!(finalized
        .metadata_transfer_staging_finalized_floors
        .contains_key(&(pg_id, second_generation)));
    finalized.validate_current_state_invariants().unwrap();
    assert_eq!(
        parse_snapshot(&format_snapshot(&finalized)).unwrap(),
        finalized
    );

    assert_eq!(
        ControlPlaneAdmin::finalize_metadata_transfer_staging_generation(
            &mut authority,
            first_cleanup.clone(),
        )
        .unwrap(),
        finalized
    );
    assert_eq!(
        ControlPlaneAdmin::finalize_metadata_transfer_staging_generation(
            &mut authority,
            second_cleanup.clone(),
        )
        .unwrap(),
        finalized
    );
    drop(authority);
    let mut restarted = SingleAuthorityControlPlane::open(store).unwrap();
    let restarted_snapshot = restarted.snapshot().clone();
    assert!(restarted_snapshot
        .metadata_transfer_staging_finalized_floors
        .contains_key(&(pg_id, first_generation)));
    assert!(restarted_snapshot
        .metadata_transfer_staging_finalized_floors
        .contains_key(&(pg_id, second_generation)));
    restarted_snapshot
        .validate_current_state_invariants()
        .unwrap();
    assert_eq!(
        ControlPlaneAdmin::finalize_metadata_transfer_staging_generation(
            &mut restarted,
            first_cleanup,
        )
        .unwrap(),
        restarted_snapshot
    );
    assert_eq!(
        ControlPlaneAdmin::finalize_metadata_transfer_staging_generation(
            &mut restarted,
            second_cleanup,
        )
        .unwrap(),
        restarted_snapshot
    );
}

fn begun_two_pg_staging_authorization_authority_fixture_with_proof(
    proof: PgMetadataProof,
) -> (
    test_util::TempDir,
    FileControlPlaneStore,
    SingleAuthorityControlPlane<FileControlPlaneStore>,
    Vec<UnavailablePgStagingIntentAuthorizationRequest>,
) {
    begun_staging_authorization_authority_fixture_with_proof(
        vec![PgId::new(70), PgId::new(71)],
        proof,
    )
}

fn begun_staging_authorization_authority_fixture_with_proof(
    pg_ids: Vec<PgId>,
    proof: PgMetadataProof,
) -> (
    test_util::TempDir,
    FileControlPlaneStore,
    SingleAuthorityControlPlane<FileControlPlaneStore>,
    Vec<UnavailablePgStagingIntentAuthorizationRequest>,
) {
    let (tmp, store, mut authority, _) = certified_spare_authority_with_policy_and_pgs(
        4,
        test_certified_storage_placement_policy((1..=4).map(NodeId::new), 3, 50),
        pg_ids.clone(),
    );
    for node_id in 1..=4 {
        heartbeat_spare_node(&mut authority, node_id, 1_000 + u64::from(node_id));
    }
    for node_id in 1..=3 {
        heartbeat_with_pg_proofs_and_lease_duration(
            &mut authority,
            node_id,
            &pg_ids,
            PgState::Peering,
            proof,
            2_000 + u64::from(node_id),
            10_000,
        );
    }
    authority.complete_ready_pg_peerings(2_004).unwrap();
    for node_id in 1..=3 {
        heartbeat_with_pg_proofs_and_lease_duration(
            &mut authority,
            node_id,
            &pg_ids,
            PgState::Active,
            proof,
            3_000 + u64::from(node_id),
            if node_id == 1 { 100 } else { 10_000 },
        );
    }
    let failed_deadline = authority
        .snapshot()
        .node(NodeId::new(1))
        .unwrap()
        .lease_deadline_ms()
        .unwrap();
    authority.expire_heartbeat_leases(failed_deadline).unwrap();
    for node_id in 2..=4 {
        heartbeat_spare_node(
            &mut authority,
            node_id,
            failed_deadline + u64::from(node_id),
        );
    }
    let proof_at_ms = authority.snapshot().max_committed_timestamp_ms().unwrap() + 1;
    for node_id in [2, 3] {
        heartbeat_with_pg_proofs_and_lease_duration(
            &mut authority,
            node_id,
            &pg_ids,
            PgState::Peering,
            proof,
            proof_at_ms + u64::from(node_id),
            10_000,
        );
    }
    let begin_at_ms = authority
        .snapshot()
        .unavailable_node_observation(NodeId::new(1))
        .unwrap()
        .observed_at_ms()
        + 50;
    authority
        .begin_unavailable_pg_placement_transition_batch(
            &pg_ids
                .iter()
                .copied()
                .map(|pg_id| (pg_id, NodeId::new(1)))
                .collect::<Vec<_>>(),
            begin_at_ms,
        )
        .unwrap();
    let authorizations = pg_ids
        .into_iter()
        .enumerate()
        .map(|(index, pg_id)| {
            let transition = authority
                .snapshot()
                .unavailable_pg_placement_transition(pg_id)
                .unwrap();
            UnavailablePgStagingIntentAuthorizationRequest {
                unavailable_transition: UnavailablePgTransitionMutationBinding::new(
                    transition.pg_id,
                    transition.transition_epoch,
                    transition.source_epoch,
                    transition.source_acting_set.clone(),
                    transition.destination_acting_set.clone(),
                ),
                staging_generation: transition.transition_epoch.get(),
                artifact_target_epoch: next_epoch(authority.snapshot().cluster_epoch()).unwrap(),
                artifact_digest: [0x80 + u8::try_from(index).unwrap(); 32],
                artifact_length: 8_192 + u64::try_from(index).unwrap(),
                artifact_format_version:
                    crate::pg_store::METADATA_TRANSFER_STAGED_ARTIFACT_FORMAT_VERSION,
            }
        })
        .collect::<Vec<_>>();
    (tmp, store, authority, authorizations)
}

pub(crate) struct AuthenticatedStagingAuthorizationFixture {
    pub(crate) destination_node_id: NodeId,
    pub(crate) runtime_map: ClusterRuntimeMapSnapshot,
    pub(crate) authorization:
        crate::control_plane_command::CommittedUnavailablePgStagingAuthorization,
    pub(crate) intent: crate::pg_store::MetadataTransferStagingIntent,
    pub(crate) artifact: Vec<u8>,
    pub(crate) initial_destination_epoch: ClusterEpoch,
    pub(crate) rebased_destination_epoch: ClusterEpoch,
    pub(crate) cross_member_runtime_map: ClusterRuntimeMapSnapshot,
    pub(crate) cross_member_authorization:
        crate::control_plane_command::CommittedUnavailablePgStagingAuthorization,
    pub(crate) cross_member_intent: crate::pg_store::MetadataTransferStagingIntent,
    pub(crate) cross_member_tombstone_runtime_map: ClusterRuntimeMapSnapshot,
    pub(crate) cross_member_tombstone_presentation:
        crate::control_plane_command::UnavailablePgStagingAuthorizationPresentation,
}

pub(crate) fn authenticated_staging_authorization_fixture(
) -> AuthenticatedStagingAuthorizationFixture {
    let (_tmp, _store, mut authority, mut authorizations) =
        begun_two_pg_staging_authorization_authority_fixture_with_proof(PgMetadataProof::current(
            23, 0x2323, 0x3434,
        ));
    let initial_destination_epoch = next_epoch(authority.snapshot().cluster_epoch()).unwrap();
    let rebased_destination_epoch = next_epoch(initial_destination_epoch).unwrap();
    let artifact = crate::pg_store::canonical_nonempty_staged_metadata_transfer_artifact_for_test(
        &authorizations[0].unavailable_transition,
        initial_destination_epoch,
    );
    authorizations[0].artifact_digest = checksum::sha256::digest(&artifact);
    authorizations[0].artifact_length = u64::try_from(artifact.len()).unwrap();
    let intent = crate::pg_store::MetadataTransferStagingIntent::for_unavailable_transition(
        &authorizations[0].unavailable_transition,
        authorizations[0].artifact_digest,
        authorizations[0].artifact_length,
        authorizations[0].artifact_format_version,
    )
    .unwrap();
    let authorized = authority
        .authorize_unavailable_pg_staging_intents_batch(&authorizations)
        .unwrap();
    let authorization = authorized
        .committed_unavailable_pg_staging_authorization(&authorizations[0], NodeId::new(4))
        .unwrap();
    let runtime_map = authorized
        .runtime_map(authorized.max_committed_timestamp_ms().unwrap() + 1)
        .unwrap();
    let destination_node_id = authorizations[0]
        .unavailable_transition
        .destination_acting_set()[0];
    let second = &authorizations[1];
    let cross_member_binding = UnavailablePgTransitionMutationBinding::new(
        second.unavailable_transition.pg_id(),
        second.unavailable_transition.transition_epoch(),
        second.unavailable_transition.source_epoch(),
        second.unavailable_transition.source_acting_set().to_vec(),
        vec![NodeId::new(5), NodeId::new(2), NodeId::new(3)],
    );
    let cross_member_artifact =
        crate::pg_store::canonical_nonempty_staged_metadata_transfer_artifact_for_test(
            &cross_member_binding,
            initial_destination_epoch,
        );
    authorizations[1] = UnavailablePgStagingIntentAuthorizationRequest {
        unavailable_transition: cross_member_binding.clone(),
        staging_generation: cross_member_binding.transition_epoch().get(),
        artifact_target_epoch: initial_destination_epoch,
        artifact_digest: checksum::sha256::digest(&cross_member_artifact),
        artifact_length: u64::try_from(cross_member_artifact.len()).unwrap(),
        artifact_format_version: crate::pg_store::METADATA_TRANSFER_STAGED_ARTIFACT_FORMAT_VERSION,
    };
    let cross_member_identity =
        unavailable_pg_staging_authorization_batch_identity(&authorizations);
    let cross_member_presentation =
        crate::control_plane_command::UnavailablePgStagingAuthorizationPresentation::from_authority_state(
            authorizations.clone(),
            runtime_map.cluster_epoch(),
            cross_member_identity.members_digest,
        )
        .unwrap();
    let cross_member_authorization =
        crate::control_plane::committed_staging_authorization_from_presentation_for_test(
            cross_member_presentation.clone(),
            NodeId::new(5),
            cross_member_binding.pg_id(),
        );
    let cross_member_intent =
        crate::pg_store::MetadataTransferStagingIntent::for_unavailable_transition(
            &cross_member_binding,
            checksum::sha256::digest(&cross_member_artifact),
            u64::try_from(cross_member_artifact.len()).unwrap(),
            crate::pg_store::METADATA_TRANSFER_STAGED_ARTIFACT_FORMAT_VERSION,
        )
        .unwrap();
    let mut cross_member_runtime_map = runtime_map.clone();
    cross_member_runtime_map.staging_authorizations = vec![cross_member_presentation];
    let mut cross_member_tombstone_authorizations = authorizations;
    let first_binding = &cross_member_tombstone_authorizations[0].unavailable_transition;
    cross_member_tombstone_authorizations[0].unavailable_transition =
        UnavailablePgTransitionMutationBinding::new(
            first_binding.pg_id(),
            first_binding.transition_epoch(),
            first_binding.source_epoch(),
            first_binding.source_acting_set().to_vec(),
            vec![NodeId::new(5), NodeId::new(2), NodeId::new(3)],
        );
    let second_binding = &cross_member_tombstone_authorizations[1].unavailable_transition;
    cross_member_tombstone_authorizations[1].unavailable_transition =
        UnavailablePgTransitionMutationBinding::new(
            second_binding.pg_id(),
            second_binding.transition_epoch(),
            second_binding.source_epoch(),
            second_binding.source_acting_set().to_vec(),
            vec![destination_node_id, NodeId::new(2), NodeId::new(3)],
        );
    let cross_member_tombstone_digest =
        unavailable_pg_staging_authorization_batch_identity(&cross_member_tombstone_authorizations)
            .members_digest;
    let cross_member_tombstone_presentation =
        crate::control_plane_command::UnavailablePgStagingAuthorizationPresentation::from_authority_state(
            cross_member_tombstone_authorizations,
            runtime_map.cluster_epoch(),
            cross_member_tombstone_digest,
        )
        .unwrap();
    let mut cross_member_tombstone_runtime_map = runtime_map.clone();
    cross_member_tombstone_runtime_map.staging_authorizations =
        vec![cross_member_tombstone_presentation.clone()];
    AuthenticatedStagingAuthorizationFixture {
        destination_node_id,
        runtime_map,
        authorization,
        intent,
        artifact,
        initial_destination_epoch,
        rebased_destination_epoch,
        cross_member_runtime_map,
        cross_member_authorization,
        cross_member_intent,
        cross_member_tombstone_runtime_map,
        cross_member_tombstone_presentation,
    }
}

#[test]
fn plural_staging_authorization_builder_and_standalone_authority_are_atomic_and_replayable() {
    let (_tmp, store, mut authority, authorizations) =
        begun_two_pg_staging_authorization_authority_fixture_with_proof(PgMetadataProof::current(
            23, 0x2323, 0x3434,
        ));
    let source = authority.snapshot().clone();
    let source_runtime = source
        .runtime_map(source.max_committed_timestamp_ms().unwrap() + 1)
        .unwrap();
    let uncommitted_presentation =
        crate::control_plane_command::UnavailablePgStagingAuthorizationPresentation::from_authority_state(
            authorizations.clone(),
            source.cluster_epoch(),
            unavailable_pg_staging_authorization_batch_identity(&authorizations).members_digest,
        )
        .unwrap();
    assert!(source_runtime
        .authority_published_staging_authorizations()
        .verify(
            NodeId::new(4),
            PgId::new(70),
            source_runtime.cluster_epoch(),
            &uncommitted_presentation,
        )
        .is_err());
    let command = source
        .authorize_unavailable_pg_staging_intents_batch_command(&authorizations)
        .unwrap();
    assert!(matches!(
        command,
        ControlPlaneCommand::AuthorizeUnavailablePgStagingIntents { authorizations: ref encoded }
            if encoded == &authorizations
    ));
    assert!(
        crate::control_plane_raft::control_plane_command_replication_encoded_len(&command).unwrap()
            <= crate::control_plane_raft::CONTROL_PLANE_RAFT_MAX_ENCODED_ENTRY_BYTES
    );

    let mut invalid = authorizations.clone();
    invalid[1].artifact_length = 0;
    let error = source
        .authorize_unavailable_pg_staging_intents_batch_command(&invalid)
        .unwrap_err();
    assert!(error
        .to_string()
        .contains("invalid generation, artifact length, or storage format"));
    assert!(source
        .authorize_unavailable_pg_staging_intents_batch_command(&[])
        .unwrap_err()
        .to_string()
        .contains("batch is empty"));
    let mut reversed = authorizations.clone();
    reversed.reverse();
    assert!(source
        .authorize_unavailable_pg_staging_intents_batch_command(&reversed)
        .unwrap_err()
        .to_string()
        .contains("not strictly increasing"));

    let authorized = <SingleAuthorityControlPlane<_> as ControlPlaneAdmin>::
        authorize_unavailable_pg_staging_intents_batch(&mut authority, &authorizations)
    .unwrap();
    assert_eq!(authorized.cluster_epoch(), source.cluster_epoch());
    for pg_id in [PgId::new(70), PgId::new(71)] {
        assert!(authorized
            .unavailable_pg_placement_transition(pg_id)
            .unwrap()
            .staging_authorization
            .is_some());
    }
    let committed = authorized
        .committed_unavailable_pg_staging_authorization(&authorizations[0], NodeId::new(4))
        .unwrap();
    let authorized_runtime = authorized
        .runtime_map(authorized.max_committed_timestamp_ms().unwrap() + 1)
        .unwrap();
    let decoded_authorized_runtime =
        super::rpc::decode_runtime_map_test_snapshot(authorized_runtime).unwrap();
    let published = decoded_authorized_runtime.authority_published_staging_authorizations();
    assert_eq!(
        published
            .verify(
                NodeId::new(4),
                authorizations[0].unavailable_transition.pg_id(),
                decoded_authorized_runtime.cluster_epoch(),
                committed.presentation(),
            )
            .unwrap(),
        committed
    );
    let regrouped_authorizations = vec![authorizations[0].clone()];
    let regrouped =
        crate::control_plane_command::UnavailablePgStagingAuthorizationPresentation::from_authority_state(
            regrouped_authorizations.clone(),
            committed.committed_epoch(),
            unavailable_pg_staging_authorization_members_digest(&regrouped_authorizations),
        )
        .unwrap();
    assert!(published
        .verify(
            NodeId::new(4),
            PgId::new(70),
            decoded_authorized_runtime.cluster_epoch(),
            &regrouped,
        )
        .is_err());
    let mut forged_runtime = decoded_authorized_runtime;
    let mut forged_digest = forged_runtime.staging_authorizations[0].batch_members_digest();
    forged_digest[0] ^= 0x80;
    forged_runtime.staging_authorizations[0].test_set_batch_members_digest(forged_digest);
    let forged_error = super::rpc::decode_runtime_map_test_snapshot(forged_runtime).unwrap_err();
    assert!(matches!(
        forged_error,
        ControlPlaneError::CommandDecode { message }
            if message.contains("batch digest is not canonical")
    ));
    let replay = authority
        .authorize_unavailable_pg_staging_intents_batch(&authorizations)
        .unwrap();
    assert_eq!(replay, authorized);
    assert_eq!(store.load().unwrap().unwrap(), authorized);
    drop(authority);
    let mut restarted = SingleAuthorityControlPlane::open(store).unwrap();
    let restarted_snapshot = restarted.snapshot().clone();
    assert_eq!(
        restarted
            .authorize_unavailable_pg_staging_intents_batch(&authorizations)
            .unwrap(),
        restarted_snapshot
    );
}

#[test]
fn plural_destination_install_builder_and_standalone_authority_are_atomic_and_replayable() {
    let (_tmp, store, mut authority, requests) = staged_two_pg_install_authority_fixture_with_proof(
        PgMetadataProof::current(23, 0x2323, 0x3434),
    );
    let source = authority.snapshot().clone();
    let destination_epoch = requests[0].expected_destination_epoch;
    let command = source
        .install_unavailable_pg_placement_transitions_batch_command(&requests, destination_epoch)
        .unwrap();
    assert!(matches!(
        command,
        ControlPlaneCommand::InstallUnavailablePgPlacementTransitions {
            ref transitions,
            expected_destination_epoch,
        } if transitions == &requests && expected_destination_epoch == destination_epoch
    ));
    assert!(
        crate::control_plane_raft::control_plane_command_replication_encoded_len(&command).unwrap()
            <= crate::control_plane_raft::CONTROL_PLANE_RAFT_MAX_ENCODED_ENTRY_BYTES
    );

    let mut invalid = requests.clone();
    invalid[1].publications[0].evidence_digest[0] ^= 0x80;
    assert!(source
        .install_unavailable_pg_placement_transitions_batch_command(&invalid, destination_epoch,)
        .unwrap_err()
        .to_string()
        .contains("digest does not match retained evidence"));
    let mut reversed = requests.clone();
    reversed.reverse();
    assert!(source
        .install_unavailable_pg_placement_transitions_batch_command(&reversed, destination_epoch,)
        .unwrap_err()
        .to_string()
        .contains("not strictly increasing"));

    let installed = <SingleAuthorityControlPlane<_> as ControlPlaneAdmin>::
        install_unavailable_pg_placement_transitions_batch(
            &mut authority,
            &requests,
            destination_epoch,
        )
    .unwrap();
    assert_eq!(installed.cluster_epoch(), destination_epoch);
    for pg_id in [PgId::new(70), PgId::new(71)] {
        assert!(installed
            .unavailable_pg_placement_transition(pg_id)
            .unwrap()
            .destination_install
            .is_some());
    }
    let replay = authority
        .install_unavailable_pg_placement_transitions_batch(&requests, destination_epoch)
        .unwrap();
    assert_eq!(replay, installed);
    assert_eq!(store.load().unwrap().unwrap(), installed);
    drop(authority);
    let mut restarted = SingleAuthorityControlPlane::open(store).unwrap();
    let restarted_snapshot = restarted.snapshot().clone();
    assert_eq!(
        restarted
            .install_unavailable_pg_placement_transitions_batch(&requests, destination_epoch)
            .unwrap(),
        restarted_snapshot
    );
}

#[test]
fn unavailable_pg_destination_install_batch_is_receipt_bound_atomic_and_replayable() {
    let (source, requests) = staged_two_pg_install_fixture();
    let destination_epoch = requests[0].expected_destination_epoch;
    let command = ControlPlaneCommand::InstallUnavailablePgPlacementTransitions {
        transitions: requests.clone(),
        expected_destination_epoch: destination_epoch,
    };
    assert_eq!(
        decode_control_plane_command(&encode_control_plane_command(&command).unwrap()).unwrap(),
        command
    );

    let singleton = source
        .install_unavailable_pg_placement_transitions_batch_command(
            std::slice::from_ref(&requests[0]),
            destination_epoch,
        )
        .unwrap();
    let singleton_len =
        crate::control_plane_raft::control_plane_command_replication_encoded_len(&singleton)
            .unwrap();
    let full_len =
        crate::control_plane_raft::control_plane_command_replication_encoded_len(&command).unwrap();
    assert!(singleton_len < full_len);
    let split_limit = singleton_len + (full_len - singleton_len) / 2;
    let split = source
        .prepare_unavailable_pg_placement_install_batch_with_replication_limit(
            &requests,
            destination_epoch,
            split_limit,
        )
        .unwrap();
    assert_eq!(split.included, vec![requests[0].clone()]);
    assert!(split.rejected.is_empty());
    assert!(
        crate::control_plane_raft::control_plane_command_replication_encoded_len(
            split.command.as_ref().unwrap()
        )
        .unwrap()
            <= split_limit
    );
    let below_singleton = source
        .prepare_unavailable_pg_placement_install_batch_with_replication_limit(
            std::slice::from_ref(&requests[0]),
            destination_epoch,
            singleton_len - 1,
        )
        .unwrap();
    assert!(below_singleton.command.is_none());
    assert!(below_singleton.included.is_empty());
    assert_eq!(below_singleton.rejected.len(), 1);

    let mut invalid = requests.clone();
    invalid[1].publications[0].evidence_digest[0] ^= 0x80;
    let error = source
        .apply_control_plane_command(
            ControlPlaneCommand::InstallUnavailablePgPlacementTransitions {
                transitions: invalid,
                expected_destination_epoch: destination_epoch,
            },
        )
        .unwrap_err();
    assert!(error
        .to_string()
        .contains("digest does not match retained evidence"));

    let mut wrong_transfer = requests.clone();
    wrong_transfer[0].transfer = PgMetadataTransferProof::new_with_imported_metadata_proof(
        wrong_transfer[0].transfer.source_epoch(),
        wrong_transfer[0].transfer.source_metadata_proof(),
        PgMetadataProof::current(23, 0x2323, 0x3435),
    );
    let error = source
        .apply_control_plane_command(
            ControlPlaneCommand::InstallUnavailablePgPlacementTransitions {
                transitions: wrong_transfer,
                expected_destination_epoch: destination_epoch,
            },
        )
        .unwrap_err();
    assert!(error
        .to_string()
        .contains("staged artifact does not derive the requested transfer proof"));

    let applied = source.apply_control_plane_command(command.clone()).unwrap();
    assert!(applied.changed());
    assert_eq!(applied.snapshot().cluster_epoch(), destination_epoch);
    for pg_id in [PgId::new(70), PgId::new(71)] {
        let transition = applied
            .snapshot()
            .unavailable_pg_placement_transition(pg_id)
            .unwrap();
        assert_eq!(transition.destination_epoch(), Some(destination_epoch));
        assert!(transition.destination_install.is_some());
    }
    applied
        .snapshot()
        .validate_current_state_invariants()
        .unwrap();
    assert_eq!(
        parse_snapshot(&format_snapshot(applied.snapshot())).unwrap(),
        *applied.snapshot()
    );

    let replay = applied
        .snapshot()
        .apply_control_plane_command(command)
        .unwrap();
    assert!(!replay.changed());
    assert_eq!(replay.snapshot(), applied.snapshot());

    let subset = ControlPlaneCommand::InstallUnavailablePgPlacementTransitions {
        transitions: vec![requests[0].clone()],
        expected_destination_epoch: destination_epoch,
    };
    assert!(applied
        .snapshot()
        .apply_control_plane_command(subset)
        .unwrap_err()
        .to_string()
        .contains("conflicts with durable install evidence"));

    let mut forged = applied.snapshot().clone();
    forged
        .unavailable_pg_placement_transitions
        .get_mut(&PgId::new(70))
        .unwrap()
        .destination_install
        .as_mut()
        .unwrap()
        .publications[0]
        .evidence_digest[0] ^= 1;
    assert!(parse_snapshot(&format_snapshot(&forged))
        .unwrap_err()
        .to_string()
        .contains("publication digest is invalid"));

    let mut coordinated_receipt_forgery = applied.snapshot().clone();
    for pg_id in [PgId::new(70), PgId::new(71)] {
        coordinated_receipt_forgery
            .unavailable_pg_placement_transitions
            .get_mut(&pg_id)
            .unwrap()
            .destination_install
            .as_mut()
            .unwrap()
            .batch_receipt
            .identity
            .members_digest[0] ^= 1;
    }
    assert!(
        parse_snapshot(&format_snapshot(&coordinated_receipt_forgery))
            .unwrap_err()
            .to_string()
            .contains("batch receipt digest does not match its durable member evidence")
    );

    let mut forged_floor_epoch = applied.snapshot().clone();
    let pg_id = PgId::new(70);
    let forged_epoch = forged_floor_epoch
        .unavailable_pg_placement_transition(pg_id)
        .unwrap()
        .source_epoch;
    forged_floor_epoch
        .pgs
        .get_mut(&pg_id)
        .unwrap()
        .peering_metadata_proof_floor_epoch = Some(forged_epoch);
    forged_floor_epoch
        .unavailable_pg_placement_transitions
        .get_mut(&pg_id)
        .unwrap()
        .destination_route
        .as_mut()
        .unwrap()
        .peering_metadata_proof_floor_epoch = Some(forged_epoch);
    assert!(parse_snapshot(&format_snapshot(&forged_floor_epoch))
        .unwrap_err()
        .to_string()
        .contains("invalid destination install evidence"));
}

#[test]
fn nonempty_staged_artifact_requires_epoch_rebound_proof_after_unrelated_advance() {
    let (probe_snapshot, probe_requests) =
        staged_two_pg_install_fixture_with_proof(PgMetadataProof::empty());
    let binding = probe_requests[0].unavailable_transition.clone();
    let pg_id = binding.pg_id();
    let initial_destination_epoch = probe_requests[0].expected_destination_epoch;
    let artifact = crate::pg_store::canonical_nonempty_staged_metadata_transfer_artifact_for_test(
        &binding,
        initial_destination_epoch,
    );
    let artifact_digest = checksum::sha256::digest(&artifact);
    let artifact_length = u64::try_from(artifact.len()).unwrap();
    let intent = crate::pg_store::MetadataTransferStagingIntent::for_unavailable_transition(
        &binding,
        artifact_digest,
        artifact_length,
        crate::pg_store::METADATA_TRANSFER_STAGED_ARTIFACT_FORMAT_VERSION,
    )
    .unwrap();
    let probe_node_id = binding.destination_acting_set()[0];
    let probe_node = probe_snapshot.node(probe_node_id).unwrap();
    let probe_temp = test_util::tempdir();
    let probe_store = crate::pg_store::MetadataTransferStagingStore::open(
        probe_temp.path(),
        crate::pg_store::MetadataTransferStagingNodeIdentity::new(
            probe_node_id,
            probe_node.node_incarnation(),
            probe_node.endpoint().to_owned(),
        )
        .unwrap(),
        crate::pg_store::MetadataTransferStagingLimits::new(8, 1 << 20, 8 << 20).unwrap(),
    )
    .unwrap();
    probe_store.create_intent(&intent).unwrap();
    let source_proof = crate::pg_store::decode_staging_evidence(
        probe_store
            .publish_artifact(&intent, &artifact)
            .unwrap()
            .as_bytes(),
    )
    .unwrap()
    .transfer()
    .unwrap()
    .source_metadata_proof();
    drop(probe_store);
    drop(probe_temp);

    let (mut snapshot, original_requests) = staged_two_pg_install_fixture_with_proof(source_proof);
    assert_eq!(original_requests[0].unavailable_transition, binding);
    assert_eq!(
        original_requests[0].expected_destination_epoch,
        initial_destination_epoch
    );
    let authorization_request = UnavailablePgStagingIntentAuthorizationRequest {
        unavailable_transition: binding.clone(),
        staging_generation: binding.transition_epoch().get(),
        artifact_target_epoch: initial_destination_epoch,
        artifact_digest,
        artifact_length,
        artifact_format_version: crate::pg_store::METADATA_TRANSFER_STAGED_ARTIFACT_FORMAT_VERSION,
    };
    let authorization_epoch = snapshot
        .unavailable_pg_placement_transition(pg_id)
        .unwrap()
        .staging_authorization
        .as_ref()
        .unwrap()
        .batch_receipt
        .source_epoch;
    let authorization_receipt = UnavailablePgTransitionBatchReceipt {
        identity: unavailable_pg_staging_authorization_batch_identity(std::slice::from_ref(
            &authorization_request,
        )),
        source_epoch: authorization_epoch,
        target_epoch: authorization_epoch,
    };
    snapshot.metadata_transfer_staging_evidence_pages.clear();
    snapshot.metadata_transfer_staging_evidence.clear();
    snapshot
        .unavailable_pg_placement_transitions
        .get_mut(&PgId::new(71))
        .unwrap()
        .staging_authorization = None;
    snapshot
        .unavailable_pg_placement_transitions
        .get_mut(&pg_id)
        .unwrap()
        .staging_authorization = Some(UnavailablePgStagingIntentAuthorization {
        staging_generation: authorization_request.staging_generation,
        artifact_target_epoch: authorization_request.artifact_target_epoch,
        artifact_digest,
        artifact_length,
        artifact_format_version: authorization_request.artifact_format_version,
        batch_receipt: authorization_receipt,
    });
    snapshot.validate_current_state_invariants().unwrap();

    let mut destination_stores = Vec::new();
    for node_id in binding.destination_acting_set().iter().copied() {
        let node = snapshot.node(node_id).unwrap();
        let actor = crate::pg_store::MetadataTransferStagingNodeIdentity::new(
            node_id,
            node.node_incarnation(),
            node.endpoint().to_owned(),
        )
        .unwrap();
        let temp = test_util::tempdir();
        let store = crate::pg_store::MetadataTransferStagingStore::open(
            temp.path(),
            actor,
            crate::pg_store::MetadataTransferStagingLimits::new(8, 1 << 20, 8 << 20).unwrap(),
        )
        .unwrap();
        store.create_intent(&intent).unwrap();
        let publication = store.publish_artifact(&intent, &artifact).unwrap();
        let page = store.next_evidence_page().unwrap().unwrap();
        let applied = snapshot
            .apply_control_plane_command(
                ControlPlaneCommand::ApplyMetadataTransferStagingEvidencePage {
                    operation_payload: page.operation_payload().to_vec(),
                    page_digest: page.page_digest(),
                },
            )
            .unwrap();
        let ControlPlaneCommandResponse::ApplyMetadataTransferStagingEvidencePage { apply_receipt } =
            applied.response()
        else {
            panic!("staging publication returned the wrong response kind");
        };
        let apply_receipt =
            crate::pg_store::decode_staging_evidence_apply_receipt(apply_receipt).unwrap();
        store
            .record_evidence_apply_receipt(&page, &apply_receipt)
            .unwrap();
        snapshot = applied.into_snapshot();
        destination_stores.push((temp, store, publication));
    }

    let initial_transfer = destination_stores
        .iter()
        .map(|(_, _, receipt)| {
            let evidence = crate::pg_store::decode_staging_evidence(receipt.as_bytes()).unwrap();
            assert_eq!(evidence.target_epoch(), Some(initial_destination_epoch));
            evidence.transfer().unwrap()
        })
        .reduce(|left, right| {
            assert_eq!(left, right);
            left
        })
        .unwrap();
    let mut initial_publications = destination_stores
        .iter()
        .map(|(_, _, receipt)| {
            let evidence = crate::pg_store::decode_staging_evidence(receipt.as_bytes()).unwrap();
            UnavailablePgStagingPublicationBinding {
                node_id: evidence.actor().node_id(),
                node_incarnation: evidence.actor().node_incarnation(),
                endpoint: evidence.actor().endpoint().to_owned(),
                evidence_digest: checksum::sha256::digest(receipt.as_bytes()),
            }
        })
        .collect::<Vec<_>>();
    initial_publications.sort_by_key(|publication| publication.node_id);

    snapshot = snapshot
        .apply_control_plane_command(ControlPlaneCommand::SetPgActingSet {
            pg_id: PgId::new(99),
            acting_set: vec![NodeId::new(2), NodeId::new(3), NodeId::new(4)],
        })
        .unwrap()
        .into_snapshot();
    assert_eq!(snapshot.cluster_epoch(), initial_destination_epoch);
    let rebound_destination_epoch = next_epoch(snapshot.cluster_epoch()).unwrap();
    let stale_error = snapshot
        .apply_control_plane_command(
            ControlPlaneCommand::InstallUnavailablePgPlacementTransitions {
                transitions: vec![UnavailablePgTransitionInstallRequest {
                    unavailable_transition: binding.clone(),
                    transfer: initial_transfer,
                    expected_destination_epoch: rebound_destination_epoch,
                    publications: initial_publications,
                }],
                expected_destination_epoch: rebound_destination_epoch,
            },
        )
        .unwrap_err();
    assert!(
        stale_error
            .to_string()
            .contains("has no committed staging publication"),
        "unexpected stale staging-proof error: {stale_error}"
    );

    let mut rebound_transfer = None;
    let mut rebound_publications = Vec::new();
    for (_, store, _) in &destination_stores {
        let receipt = store
            .publish_proof_for_epoch(&intent, rebound_destination_epoch)
            .unwrap();
        let evidence = crate::pg_store::decode_staging_evidence(receipt.as_bytes()).unwrap();
        assert_eq!(evidence.target_epoch(), Some(rebound_destination_epoch));
        assert_ne!(evidence.transfer(), Some(initial_transfer));
        match rebound_transfer {
            Some(expected) => assert_eq!(evidence.transfer(), Some(expected)),
            None => rebound_transfer = evidence.transfer(),
        }
        let page = store.next_evidence_page().unwrap().unwrap();
        let applied = snapshot
            .apply_control_plane_command(
                ControlPlaneCommand::ApplyMetadataTransferStagingEvidencePage {
                    operation_payload: page.operation_payload().to_vec(),
                    page_digest: page.page_digest(),
                },
            )
            .unwrap();
        let ControlPlaneCommandResponse::ApplyMetadataTransferStagingEvidencePage { apply_receipt } =
            applied.response()
        else {
            panic!("staging proof publication returned the wrong response kind");
        };
        store
            .record_evidence_apply_receipt(
                &page,
                &crate::pg_store::decode_staging_evidence_apply_receipt(apply_receipt).unwrap(),
            )
            .unwrap();
        snapshot = applied.into_snapshot();
        rebound_publications.push(UnavailablePgStagingPublicationBinding {
            node_id: evidence.actor().node_id(),
            node_incarnation: evidence.actor().node_incarnation(),
            endpoint: evidence.actor().endpoint().to_owned(),
            evidence_digest: checksum::sha256::digest(receipt.as_bytes()),
        });
    }
    rebound_publications.sort_by_key(|publication| publication.node_id);
    let rebound_transfer = rebound_transfer.unwrap();
    let install = UnavailablePgTransitionInstallRequest {
        unavailable_transition: binding.clone(),
        transfer: rebound_transfer,
        expected_destination_epoch: rebound_destination_epoch,
        publications: rebound_publications,
    };
    let applied = snapshot
        .apply_control_plane_command(
            ControlPlaneCommand::InstallUnavailablePgPlacementTransitions {
                transitions: vec![install.clone()],
                expected_destination_epoch: rebound_destination_epoch,
            },
        )
        .unwrap();
    assert!(applied.changed());
    assert_eq!(
        applied.snapshot().cluster_epoch(),
        rebound_destination_epoch
    );
    let control_plane_tmp = test_util::tempdir();
    let control_plane_store =
        FileControlPlaneStore::new(control_plane_tmp.path().join("control-plane.state"));
    control_plane_store
        .checkpoint(None, applied.snapshot())
        .unwrap();
    let mut authority = SingleAuthorityControlPlane::open(control_plane_store.clone()).unwrap();
    complete_installed_unavailable_transition(&mut authority, &install);
    assert_eq!(
        authority
            .snapshot()
            .retained_unavailable_pg_placement_transitions
            .get(&(pg_id, binding.transition_epoch()))
            .unwrap()
            .destination_epoch,
        Some(rebound_destination_epoch)
    );

    let mut tombstones = Vec::new();
    let mut tombstone_pages = BTreeMap::new();
    for (_, store, _) in &destination_stores {
        let receipt = store.tombstone(&intent).unwrap();
        let evidence = crate::pg_store::decode_staging_evidence(receipt.as_bytes()).unwrap();
        let page = store.next_evidence_page().unwrap().unwrap();
        let apply_receipt = ControlPlaneAdmin::apply_metadata_transfer_staging_evidence_page(
            &mut authority,
            page.operation_payload().to_vec(),
            page.page_digest(),
        )
        .unwrap();
        store
            .record_evidence_apply_receipt(
                &page,
                &crate::pg_store::decode_staging_evidence_apply_receipt(&apply_receipt).unwrap(),
            )
            .unwrap();
        tombstone_pages.insert(evidence.actor().node_id(), page.generation());
        tombstones.push(MetadataTransferStagingTombstoneBinding {
            node_id: evidence.actor().node_id(),
            node_incarnation: evidence.actor().node_incarnation(),
            endpoint: evidence.actor().endpoint().to_owned(),
            evidence_digest: checksum::sha256::digest(receipt.as_bytes()),
        });
    }
    tombstones.sort_by_key(|tombstone| tombstone.node_id);

    let filler_transition = authority
        .snapshot()
        .unavailable_pg_placement_transition(PgId::new(71))
        .unwrap();
    let filler_binding = UnavailablePgTransitionMutationBinding::new(
        filler_transition.pg_id,
        filler_transition.transition_epoch,
        filler_transition.source_epoch,
        filler_transition.source_acting_set.clone(),
        filler_transition.destination_acting_set.clone(),
    );
    let filler_authorization = UnavailablePgStagingIntentAuthorizationRequest {
        unavailable_transition: filler_binding.clone(),
        staging_generation: filler_binding.transition_epoch().get(),
        artifact_target_epoch: next_epoch(authority.snapshot().cluster_epoch()).unwrap(),
        artifact_digest: [0xe1; 32],
        artifact_length: 4_097,
        artifact_format_version: crate::pg_store::METADATA_TRANSFER_STAGED_ARTIFACT_FORMAT_VERSION,
    };
    authority
        .authorize_unavailable_pg_staging_intents_batch(std::slice::from_ref(&filler_authorization))
        .unwrap();
    for node_id in filler_binding.destination_acting_set().iter().copied() {
        let node = authority.snapshot().node(node_id).unwrap();
        let actor = crate::pg_store::MetadataTransferStagingNodeIdentity::new(
            node_id,
            node.node_incarnation(),
            node.endpoint().to_owned(),
        )
        .unwrap();
        let previous = latest_staging_evidence_apply_receipt(
            authority.snapshot(),
            node_id,
            node.node_incarnation(),
        )
        .unwrap();
        let page = crate::pg_store::metadata_transfer_staging_evidence_page_for_test(
            actor,
            &filler_binding,
            filler_authorization.artifact_digest,
            filler_authorization.artifact_length,
            filler_authorization.artifact_format_version,
            crate::pg_store::MetadataTransferStagingEvidenceKind::Tombstone,
            Some(&previous),
        );
        ControlPlaneAdmin::apply_metadata_transfer_staging_evidence_page(
            &mut authority,
            page.operation_payload().to_vec(),
            page.page_digest(),
        )
        .unwrap();
    }
    for (node_id, last_generation) in tombstone_pages {
        let node_incarnation = authority
            .snapshot()
            .node(node_id)
            .unwrap()
            .node_incarnation();
        ControlPlaneAdmin::checkpoint_metadata_transfer_staging_evidence_pages(
            &mut authority,
            node_id,
            node_incarnation,
            1,
            last_generation,
        )
        .unwrap();
    }
    let cleanup = FinalizeMetadataTransferStagingGenerationRequest {
        unavailable_transition: binding,
        staging_generation: intent.staging_generation(),
        disposition: MetadataTransferStagingCleanupDisposition::Completed,
        tombstones,
    };
    ControlPlaneAdmin::finalize_metadata_transfer_staging_generation(
        &mut authority,
        cleanup.clone(),
    )
    .unwrap();
    let floor = &authority
        .snapshot()
        .metadata_transfer_staging_finalized_floors[&(pg_id, intent.staging_generation())];
    assert_eq!(
        floor
            .publications
            .iter()
            .map(|publication| publication.target_epoch)
            .collect::<BTreeSet<_>>(),
        BTreeSet::from([initial_destination_epoch, rebound_destination_epoch])
    );
    assert!(authority
        .snapshot()
        .metadata_transfer_staging_evidence
        .keys()
        .all(|key| key.pg_id != pg_id));
    authority
        .snapshot()
        .validate_current_state_invariants()
        .unwrap();
    let mut forged_historical_publication = authority.snapshot().clone();
    let publication = forged_historical_publication
        .metadata_transfer_staging_finalized_floors
        .get_mut(&(pg_id, intent.staging_generation()))
        .unwrap()
        .publications
        .iter_mut()
        .find(|publication| publication.target_epoch == initial_destination_epoch)
        .unwrap();
    publication.transfer = rebound_transfer;
    assert!(
        parse_snapshot(&format_snapshot(&forged_historical_publication))
            .unwrap_err()
            .to_string()
            .contains("not canonically committed")
    );
    let expected_publications = floor.publications.clone();
    drop(authority);
    let mut restarted = SingleAuthorityControlPlane::open(control_plane_store).unwrap();
    let replay =
        ControlPlaneAdmin::finalize_metadata_transfer_staging_generation(&mut restarted, cleanup)
            .unwrap();
    assert_eq!(
        replay.metadata_transfer_staging_finalized_floors[&(pg_id, intent.staging_generation())]
            .publications,
        expected_publications
    );
}

#[test]
fn finalized_evidence_index_accepts_every_actor_at_the_epoch_proof_limit() {
    let (_, requests) = staged_two_pg_install_fixture_with_proof(PgMetadataProof::empty());
    let transition = requests[0].unavailable_transition.clone();
    let pg_id = transition.pg_id();
    let staging_generation = transition.transition_epoch().get();
    let transfer = PgMetadataTransferProof::new(
        transition.source_epoch(),
        PgMetadataProof::current(1, 0x12, 0x34),
    );
    let mut publications = Vec::new();
    for offset in 1..=crate::pg_store::MAX_STAGING_EPOCH_PROOFS_PER_INTENT {
        let target_epoch =
            ClusterEpoch::new(transition.transition_epoch().get() + u64::try_from(offset).unwrap())
                .unwrap();
        for node_id in transition.destination_acting_set().iter().copied() {
            publications.push(MetadataTransferStagingFinalizedPublicationBinding {
                node_id,
                node_incarnation: 1,
                endpoint: format!("/tmp/transition-node-{}.sock", node_id.as_u32()),
                target_epoch,
                transfer,
                evidence_digest: [u8::try_from(offset).unwrap(); 32],
            });
        }
    }
    let floor_key = (pg_id, staging_generation);
    let floor = MetadataTransferStagingFinalizedFloor {
        transition: transition.clone(),
        staging_generation,
        disposition: MetadataTransferStagingCleanupDisposition::Completed,
        artifact_digest: [0x45; 32],
        artifact_length: 4_096,
        artifact_format_version: crate::pg_store::METADATA_TRANSFER_STAGED_ARTIFACT_FORMAT_VERSION,
        publications,
        tombstones: Vec::new(),
        tombstone_set_digest: [0x67; 32],
        checkpoint_bindings: BTreeMap::new(),
    };
    let mut floors = BTreeMap::from([(floor_key, floor)]);
    let index = metadata_transfer_staging_finalized_evidence_index(&floors).unwrap();
    assert_eq!(
        index.len(),
        crate::pg_store::MAX_STAGING_EPOCH_PROOFS_PER_INTENT
            * transition.destination_acting_set().len()
    );
    for publication in &floors[&floor_key].publications {
        let key = MetadataTransferStagingEvidenceKey {
            pg_id,
            staging_generation,
            actor_node_id: publication.node_id,
            actor_node_incarnation: publication.node_incarnation,
            kind: crate::pg_store::MetadataTransferStagingEvidenceKind::Publication,
            target_epoch: Some(publication.target_epoch),
        };
        let indexed = index.get(&key).unwrap();
        assert!(std::ptr::eq(indexed.floor, &floors[&floor_key]));
        assert_eq!(
            (
                indexed.endpoint,
                indexed.evidence_digest,
                indexed.target_epoch,
                indexed.transfer,
            ),
            (
                publication.endpoint.as_str(),
                publication.evidence_digest,
                Some(publication.target_epoch),
                Some(publication.transfer),
            )
        );
    }
    drop(index);

    let first_node = transition.destination_acting_set()[0];
    floors.get_mut(&floor_key).unwrap().publications.push(
        MetadataTransferStagingFinalizedPublicationBinding {
            node_id: first_node,
            node_incarnation: 1,
            endpoint: format!("/tmp/transition-node-{}.sock", first_node.as_u32()),
            target_epoch: ClusterEpoch::new(
                transition.transition_epoch().get()
                    + u64::try_from(crate::pg_store::MAX_STAGING_EPOCH_PROOFS_PER_INTENT).unwrap()
                    + 1,
            )
            .unwrap(),
            transfer,
            evidence_digest: [0xff; 32],
        },
    );
    assert!(metadata_transfer_staging_finalized_evidence_index(&floors)
        .unwrap_err()
        .contains("bounded actor-target index"));
}

#[test]
fn unavailable_pg_reconciliation_scan_is_cursor_and_page_bounded() {
    let nodes = vec![(NodeId::new(1), "/tmp/reconcile-node-1.sock".to_owned())];
    let pgs = (1..=17)
        .map(|pg_id| (PgId::new(pg_id), vec![NodeId::new(1)]))
        .collect::<Vec<_>>();
    let topology = InitialClusterTopologyCertificate::new_for_bootstrap_map(
        3,
        [0x5b; CONTROL_PLANE_TOPOLOGY_DIGEST_LEN],
        vec![1],
        &nodes,
        &pgs,
        test_certified_storage_placement_policy([NodeId::new(1)], 1, 50),
    )
    .unwrap();
    let snapshot = ClusterControlSnapshot::empty()
        .apply_control_plane_command(ControlPlaneCommand::BootstrapCertifiedInitialClusterMap {
            nodes,
            pg_acting_sets: pgs,
            topology,
        })
        .unwrap()
        .into_snapshot();

    let first = snapshot
        .scan_unavailable_pg_reconciliation(UnavailablePgReconciliationCursor::start(), 1_000);
    assert!(first.candidate.is_none());
    assert_eq!(first.next_cursor.after_pg_id(), Some(PgId::new(16)));
    let second = snapshot.scan_unavailable_pg_reconciliation(first.next_cursor, 1_000);
    assert!(second.candidate.is_none());
    assert_eq!(
        second.next_cursor,
        UnavailablePgReconciliationCursor::start()
    );
}

#[test]
fn active_successor_reconciliation_outranks_retained_terminal_cleanup() {
    let (_tmp, _store, mut authority, installs) = completed_staged_two_pg_authority_fixture();
    let pg_id = installs[0].unavailable_transition.pg_id();
    let before = authority.snapshot().scan_unavailable_pg_reconciliation(
        UnavailablePgReconciliationCursor::start(),
        authority.snapshot().max_committed_timestamp_ms().unwrap(),
    );
    let Some(UnavailablePgReconciliationCandidate::Resume(cleanup)) = before.candidate else {
        panic!("completed staged transition did not expose terminal cleanup");
    };
    assert_eq!(cleanup.pg_id(), pg_id);
    assert_eq!(
        cleanup.stage(),
        UnavailablePgReconciliationStage::StagingCleanup
    );

    let successor = begin_successor_unavailable_transition(
        &mut authority,
        pg_id,
        NodeId::new(4),
        installs[0].transfer.metadata_proof(),
    );
    let after = authority.snapshot().scan_unavailable_pg_reconciliation(
        UnavailablePgReconciliationCursor::start(),
        authority.snapshot().max_committed_timestamp_ms().unwrap(),
    );
    let Some(UnavailablePgReconciliationCandidate::Resume(active)) = after.candidate else {
        panic!("active successor was hidden by retained terminal cleanup");
    };
    assert_eq!(active.pg_id(), pg_id);
    assert_eq!(active.transition_epoch(), successor.transition_epoch());
    assert_eq!(
        active.stage(),
        UnavailablePgReconciliationStage::MetadataTransfer
    );

    let batch = authority
        .snapshot()
        .scan_unavailable_pg_reconciliation_batch(
            UnavailablePgReconciliationCursor::start(),
            authority.snapshot().max_committed_timestamp_ms().unwrap(),
        );
    assert!(batch.candidates.iter().any(|candidate| matches!(
        candidate,
        UnavailablePgReconciliationCandidate::Resume(work)
            if work.pg_id() == pg_id
                && work.transition_epoch() == successor.transition_epoch()
    )));
    let fallback = batch
        .cleanup_fallbacks
        .iter()
        .find(|work| work.pg_id() == pg_id)
        .expect("active successor has no retained cleanup fallback");
    assert_eq!(fallback.transition_epoch(), cleanup.transition_epoch());
    assert_eq!(
        fallback.stage(),
        UnavailablePgReconciliationStage::StagingCleanup
    );
}

#[test]
fn superseded_cleanup_rejects_a_durable_destination_install_even_without_completion() {
    let (_tmp, _store, mut authority, installs) = completed_staged_two_pg_authority_fixture();
    let install = &installs[0];
    let pg_id = install.unavailable_transition.pg_id();
    let transition_epoch = install.unavailable_transition.transition_epoch();
    let successor = begin_successor_unavailable_transition(
        &mut authority,
        pg_id,
        NodeId::new(4),
        install.transfer.metadata_proof(),
    );
    let transition = authority
        .snapshot
        .retained_unavailable_pg_placement_transitions
        .get_mut(&(pg_id, transition_epoch))
        .unwrap();
    transition.completion = None;
    transition.completion_batch_receipt = None;

    let error = match authority
        .snapshot()
        .validate_unavailable_pg_staging_cleanup(
            &install.unavailable_transition,
            MetadataTransferStagingCleanupDisposition::Superseded {
                successor_transition_epoch: successor.transition_epoch(),
            },
            None,
            transition_epoch.get(),
        ) {
        Ok(_) => panic!("superseded cleanup accepted a durable destination install"),
        Err(error) => error,
    };
    assert!(error
        .to_string()
        .contains("not a superseded pre-install transition"));
}

#[test]
fn unavailable_pg_transition_batch_limit_is_owned_by_command_mutation() {
    let maximum = (1..=MAX_UNAVAILABLE_PG_TRANSITION_BATCH)
        .map(|pg_id| PgId::new(u32::try_from(pg_id).unwrap()))
        .collect::<Vec<_>>();
    validate_canonical_unavailable_pg_batch("test", maximum).unwrap();

    let oversized = (1..=MAX_UNAVAILABLE_PG_TRANSITION_BATCH + 1)
        .map(|pg_id| PgId::new(u32::try_from(pg_id).unwrap()))
        .collect::<Vec<_>>();
    assert!(validate_canonical_unavailable_pg_batch("test", oversized)
        .unwrap_err()
        .to_string()
        .contains("member limit"));
}

#[test]
fn unavailable_pg_begin_batch_is_atomic_durable_and_requires_whole_batch_replay() {
    let pg_ids = [PgId::new(7), PgId::new(8)];
    let (_tmp, _store, mut authority, _) = certified_spare_authority_with_policy_and_pgs(
        4,
        test_certified_storage_placement_policy((1..=4).map(NodeId::new), 3, 50),
        pg_ids.to_vec(),
    );
    for node_id in 1..=4 {
        heartbeat_spare_node(&mut authority, node_id, 1_000 + u64::from(node_id));
    }
    let active_proof = PgMetadataProof::current(17, 0x1717, 0x2727);
    for node_id in 1..=3 {
        heartbeat_with_pg_proofs_and_lease_duration(
            &mut authority,
            node_id,
            &pg_ids,
            PgState::Peering,
            active_proof,
            2_000 + u64::from(node_id),
            10_000,
        );
    }
    authority.complete_ready_pg_peerings(2_004).unwrap();
    for node_id in 1..=3 {
        heartbeat_with_pg_proofs_and_lease_duration(
            &mut authority,
            node_id,
            &pg_ids,
            PgState::Active,
            active_proof,
            3_000 + u64::from(node_id),
            if node_id == 1 { 100 } else { 10_000 },
        );
    }
    let failed_deadline = authority
        .snapshot()
        .node(NodeId::new(1))
        .unwrap()
        .lease_deadline_ms()
        .unwrap();
    authority.expire_heartbeat_leases(failed_deadline).unwrap();
    for node_id in 2..=4 {
        heartbeat_spare_node(
            &mut authority,
            node_id,
            failed_deadline + u64::from(node_id),
        );
    }
    let proof_at_ms = authority.snapshot().max_committed_timestamp_ms().unwrap() + 1;
    for node_id in [2, 3] {
        heartbeat_with_pg_proofs_and_lease_duration(
            &mut authority,
            node_id,
            &pg_ids,
            PgState::Peering,
            active_proof,
            proof_at_ms + u64::from(node_id),
            10_000,
        );
    }
    let begin_at_ms = authority
        .snapshot()
        .unavailable_node_observation(NodeId::new(1))
        .unwrap()
        .observed_at_ms()
        + 50;
    let source = authority.snapshot().clone();
    let target_epoch = next_epoch(source.cluster_epoch()).unwrap();
    let requests = pg_ids
        .into_iter()
        .map(|pg_id| {
            begin_request_from_command(
                source
                    .begin_unavailable_pg_placement_transition_command(
                        pg_id,
                        NodeId::new(1),
                        begin_at_ms,
                    )
                    .unwrap(),
            )
        })
        .collect::<Vec<_>>();

    let mut invalid_requests = requests.clone();
    invalid_requests[1].destination_acting_set.swap(0, 1);
    assert!(
        source
            .validate_unavailable_pg_transition_begin_batch(
                invalid_requests,
                target_epoch,
                begin_at_ms,
            )
            .is_err()
    );
    assert_eq!(source, *authority.snapshot());

    let prepared_with_invalid_member = source
        .prepare_unavailable_pg_placement_transition_batch(
            &[
                (PgId::new(6), NodeId::new(1)),
                (pg_ids[0], NodeId::new(1)),
                (pg_ids[1], NodeId::new(1)),
            ],
            begin_at_ms,
        )
        .unwrap();
    assert_eq!(
        prepared_with_invalid_member.included,
        pg_ids.map(|pg_id| (pg_id, NodeId::new(1))).to_vec(),
        "unexpected preparation rejections: {:?}",
        prepared_with_invalid_member.rejected
    );
    assert_eq!(prepared_with_invalid_member.rejected.len(), 1);
    assert_eq!(prepared_with_invalid_member.rejected[0].0, PgId::new(6));
    assert!(prepared_with_invalid_member.command.is_some());

    let mut large_source = source.clone();
    let large_endpoint = "x".repeat(40_000);
    large_source
        .unavailable_node_observations
        .get_mut(&NodeId::new(1))
        .unwrap()
        .endpoint = large_endpoint.clone();
    large_source
        .nodes
        .get_mut(&NodeId::new(1))
        .unwrap()
        .endpoint = large_endpoint;
    let oversized_batch = large_source
        .begin_unavailable_pg_placement_transition_batch_command(
            &pg_ids.map(|pg_id| (pg_id, NodeId::new(1))),
            begin_at_ms,
        )
        .unwrap();
    let oversized_batch_len =
        crate::control_plane_raft::control_plane_command_replication_encoded_len(&oversized_batch)
            .unwrap();
    assert!(
        oversized_batch_len > crate::control_plane_raft::CONTROL_PLANE_RAFT_MAX_ENCODED_ENTRY_BYTES,
        "two-member begin entry encoded to {oversized_batch_len} bytes"
    );
    let split = large_source
        .prepare_unavailable_pg_placement_transition_batch(
            &pg_ids.map(|pg_id| (pg_id, NodeId::new(1))),
            begin_at_ms,
        )
        .unwrap();
    assert_eq!(
        split.included,
        vec![(pg_ids[0], NodeId::new(1))],
        "split rejections: {:?}",
        split.rejected
    );
    assert!(split.rejected.is_empty());
    assert!(
        crate::control_plane_raft::control_plane_command_replication_encoded_len(
            split.command.as_ref().unwrap()
        )
        .unwrap()
            <= crate::control_plane_raft::CONTROL_PLANE_RAFT_MAX_ENCODED_ENTRY_BYTES
    );

    let validated = source
        .validate_unavailable_pg_transition_begin_batch(requests.clone(), target_epoch, begin_at_ms)
        .unwrap();
    let applied = source
        .apply_validated_unavailable_pg_transition_begins(validated, target_epoch, begin_at_ms)
        .unwrap()
        .expect("new batch must mutate the snapshot");
    assert_eq!(applied.cluster_epoch(), target_epoch);
    assert_eq!(
        applied
            .unavailable_pg_placement_transitions()
            .map(|transition| (transition.pg_id, transition.transition_epoch))
            .collect::<Vec<_>>(),
        vec![(PgId::new(7), target_epoch), (PgId::new(8), target_epoch)]
    );
    let expected_recorded = applied_control_plane_command(
        &source,
        applied,
        ControlPlaneCommandResponse::BeginUnavailablePgPlacementTransitions,
        true,
    )
    .into_snapshot();
    let exact_replay_command = ControlPlaneCommand::BeginUnavailablePgPlacementTransitions {
        transitions: requests.clone(),
        expected_transition_epoch: target_epoch,
        begin_at_ms,
    };
    let mut reconciliation_cursor = UnavailablePgReconciliationCursor::start();
    let begin_metrics_before = observability::unavailable_pg_batch_metrics_snapshot()
        .into_iter()
        .find(|sample| sample.stage == observability::UnavailablePgBatchStage::Begin)
        .unwrap();
    let begun_work = authority
        .poll_unavailable_pg_reconciliation_batch(&mut reconciliation_cursor, begin_at_ms)
        .unwrap();
    let begin_metrics_after_apply = observability::unavailable_pg_batch_metrics_snapshot()
        .into_iter()
        .find(|sample| sample.stage == observability::UnavailablePgBatchStage::Begin)
        .unwrap();
    assert_eq!(
        begin_metrics_after_apply.submitted_total,
        begin_metrics_before.submitted_total + 1
    );
    assert_eq!(
        begin_metrics_after_apply.applied_total,
        begin_metrics_before.applied_total + 1
    );
    assert_eq!(
        begin_metrics_after_apply.members_total,
        begin_metrics_before.members_total + 2
    );
    assert_eq!(
        begin_metrics_after_apply.epoch_advance_total,
        begin_metrics_before.epoch_advance_total + 1
    );
    assert!(begun_work.rejected.is_empty());
    assert_eq!(
        begun_work
            .work
            .iter()
            .map(UnavailablePgReconciliationWork::pg_id)
            .collect::<Vec<_>>(),
        pg_ids
    );
    let replayed_at_durable_boundary = authority
        .apply_control_plane_command_for_test(exact_replay_command.clone())
        .unwrap();
    assert!(!replayed_at_durable_boundary.changed());
    let begin_metrics_after_replay = observability::unavailable_pg_batch_metrics_snapshot()
        .into_iter()
        .find(|sample| sample.stage == observability::UnavailablePgBatchStage::Begin)
        .unwrap();
    assert_eq!(
        begin_metrics_after_replay.submitted_total,
        begin_metrics_before.submitted_total + 2
    );
    assert_eq!(
        begin_metrics_after_replay.replayed_total,
        begin_metrics_before.replayed_total + 1
    );
    assert_eq!(
        begin_metrics_after_replay.members_total,
        begin_metrics_before.members_total + 4
    );
    assert_eq!(
        begin_metrics_after_replay.epoch_advance_total,
        begin_metrics_after_apply.epoch_advance_total
    );
    let subset_replay = ControlPlaneCommand::BeginUnavailablePgPlacementTransitions {
        transitions: vec![requests[0].clone()],
        expected_transition_epoch: target_epoch,
        begin_at_ms,
    };
    let rejected_at_durable_boundary = authority
        .apply_control_plane_command_for_test(subset_replay.clone())
        .unwrap_err();
    assert!(
        rejected_at_durable_boundary
            .to_string()
            .contains("does not consume the active transition tip"),
        "unexpected subset replay error: {rejected_at_durable_boundary}"
    );
    let begin_metrics_after_rejection = observability::unavailable_pg_batch_metrics_snapshot()
        .into_iter()
        .find(|sample| sample.stage == observability::UnavailablePgBatchStage::Begin)
        .unwrap();
    assert_eq!(
        begin_metrics_after_rejection.submitted_total,
        begin_metrics_before.submitted_total + 3
    );
    assert_eq!(
        begin_metrics_after_rejection.rejected_total,
        begin_metrics_before.rejected_total + 1
    );
    assert_eq!(
        begin_metrics_after_rejection.members_total,
        begin_metrics_before.members_total + 5
    );
    assert_eq!(
        begin_metrics_after_rejection.epoch_advance_total,
        begin_metrics_after_apply.epoch_advance_total
    );
    let recorded = authority.snapshot().clone();
    assert_eq!(recorded, expected_recorded);
    recorded.validate_current_state_invariants().unwrap();
    assert_eq!(
        parse_snapshot(&format_snapshot(&recorded)).unwrap(),
        recorded
    );

    let replayed = recorded
        .apply_control_plane_command(exact_replay_command)
        .unwrap();
    assert!(!replayed.changed());
    assert_eq!(replayed.snapshot(), &recorded);

    let subset_error = recorded
        .apply_control_plane_command(subset_replay)
        .unwrap_err();
    assert!(
        subset_error
            .to_string()
            .contains("does not consume the active transition tip"),
        "unexpected subset replay error: {subset_error}"
    );

    let duplicate = vec![requests[0].clone(), requests[0].clone()];
    assert!(source
        .validate_unavailable_pg_transition_begin_batch(duplicate, target_epoch, begin_at_ms)
        .unwrap_err()
        .to_string()
        .contains("strictly increasing"));

    let staging_requests = pg_ids
        .into_iter()
        .enumerate()
        .map(|(index, pg_id)| {
            let transition = recorded.unavailable_pg_placement_transition(pg_id).unwrap();
            UnavailablePgStagingIntentAuthorizationRequest {
                unavailable_transition: UnavailablePgTransitionMutationBinding::new(
                    transition.pg_id,
                    transition.transition_epoch,
                    transition.source_epoch,
                    transition.source_acting_set.clone(),
                    transition.destination_acting_set.clone(),
                ),
                staging_generation: transition.transition_epoch.get(),
                artifact_target_epoch: next_epoch(recorded.cluster_epoch()).unwrap(),
                artifact_digest: [u8::try_from(index + 1).unwrap(); 32],
                artifact_length: 4_096 + u64::try_from(index).unwrap(),
                artifact_format_version:
                    crate::pg_store::METADATA_TRANSFER_STAGED_ARTIFACT_FORMAT_VERSION,
            }
        })
        .collect::<Vec<_>>();
    let staging_command = ControlPlaneCommand::AuthorizeUnavailablePgStagingIntents {
        authorizations: staging_requests.clone(),
    };
    let mut invalid_artifact = staging_requests.clone();
    invalid_artifact[1].artifact_length = 0;
    assert!(recorded
        .apply_control_plane_command(ControlPlaneCommand::AuthorizeUnavailablePgStagingIntents {
            authorizations: invalid_artifact,
        })
        .unwrap_err()
        .to_string()
        .contains("invalid generation, artifact length, or storage format"));
    let mut oversized_artifact = staging_requests.clone();
    oversized_artifact[1].artifact_length =
        crate::pg_store::METADATA_TRANSFER_STAGED_ARTIFACT_MAX_BYTES + 1;
    assert!(recorded
        .apply_control_plane_command(ControlPlaneCommand::AuthorizeUnavailablePgStagingIntents {
            authorizations: oversized_artifact,
        })
        .unwrap_err()
        .to_string()
        .contains("invalid generation, artifact length, or storage format"));
    let mut invalid_format = staging_requests.clone();
    invalid_format[1].artifact_format_version += 1;
    assert!(recorded
        .apply_control_plane_command(ControlPlaneCommand::AuthorizeUnavailablePgStagingIntents {
            authorizations: invalid_format,
        })
        .unwrap_err()
        .to_string()
        .contains("invalid generation, artifact length, or storage format"));
    let staged = recorded
        .apply_control_plane_command(staging_command.clone())
        .unwrap();
    assert!(staged.changed());
    assert_eq!(staged.snapshot().cluster_epoch(), recorded.cluster_epoch());
    staged
        .snapshot()
        .validate_current_state_invariants()
        .unwrap();
    assert_eq!(
        parse_snapshot(&format_snapshot(staged.snapshot())).unwrap(),
        *staged.snapshot()
    );
    let staging_replay = staged
        .snapshot()
        .apply_control_plane_command(staging_command.clone())
        .unwrap();
    assert!(!staging_replay.changed());
    assert_eq!(staging_replay.snapshot(), staged.snapshot());

    let evidence_request = &staging_requests[0];
    let evidence_actor_id = evidence_request
        .unavailable_transition
        .destination_acting_set()[0];
    let evidence_actor_record = staged.snapshot().node(evidence_actor_id).unwrap();
    let evidence_actor = crate::pg_store::MetadataTransferStagingNodeIdentity::new(
        evidence_actor_id,
        evidence_actor_record.node_incarnation(),
        evidence_actor_record.endpoint().to_owned(),
    )
    .unwrap();
    assert!(evidence_actor.node_incarnation() > 1);
    for rollover_boundary in 0..3 {
        let mut rollover_snapshot = staged.snapshot().clone();
        let staging_tmp = test_util::tempdir();
        let old_actor = crate::pg_store::MetadataTransferStagingNodeIdentity::new(
            evidence_actor_id,
            evidence_actor.node_incarnation() - 1,
            format!("/tmp/old-staging-actor-{rollover_boundary}.sock"),
        )
        .unwrap();
        let limits =
            crate::pg_store::MetadataTransferStagingLimits::new(8, 1 << 20, 8 << 20).unwrap();
        let store = crate::pg_store::MetadataTransferStagingStore::open(
            staging_tmp.path(),
            old_actor.clone(),
            limits,
        )
        .unwrap();
        let intent = crate::pg_store::MetadataTransferStagingIntent::for_unavailable_transition(
            &evidence_request.unavailable_transition,
            evidence_request.artifact_digest,
            evidence_request.artifact_length,
            evidence_request.artifact_format_version,
        )
        .unwrap();
        store.tombstone(&intent).unwrap();
        if rollover_boundary != 0 {
            let old_page = store.next_evidence_page().unwrap().unwrap();
            assert_eq!(old_page.actor(), &old_actor);
            if rollover_boundary == 2 {
                let old_receipt =
                    crate::pg_store::MetadataTransferStagingEvidenceApplyReceipt::for_page(
                        &old_page,
                    );
                store
                    .record_evidence_apply_receipt(&old_page, &old_receipt)
                    .unwrap();
                retain_staging_evidence_page_for_test(&mut rollover_snapshot, &old_page);
            }
        }
        drop(store);

        let reopened = crate::pg_store::MetadataTransferStagingStore::open(
            staging_tmp.path(),
            evidence_actor.clone(),
            limits,
        )
        .unwrap();
        let rebound_page = reopened.next_evidence_page().unwrap().unwrap();
        assert_eq!(rebound_page.actor(), &evidence_actor);
        assert_eq!(rebound_page.previous_generation(), 0);
        assert_eq!(rebound_page.generation(), 1);
        let rebound_evidence =
            crate::pg_store::decode_staging_evidence(rebound_page.entries()[0].evidence()).unwrap();
        assert_eq!(rebound_evidence.actor(), &evidence_actor);
        assert!(rollover_snapshot
            .apply_control_plane_command(
                ControlPlaneCommand::ApplyMetadataTransferStagingEvidencePage {
                    operation_payload: rebound_page.operation_payload().to_vec(),
                    page_digest: rebound_page.page_digest(),
                }
            )
            .unwrap()
            .changed());
    }
    let response_loss_tmp = test_util::tempdir();
    let response_loss_old_actor = crate::pg_store::MetadataTransferStagingNodeIdentity::new(
        evidence_actor_id,
        evidence_actor.node_incarnation() - 1,
        "/tmp/response-loss-old-staging-actor.sock".to_owned(),
    )
    .unwrap();
    let response_loss_store = crate::pg_store::MetadataTransferStagingStore::open(
        response_loss_tmp.path(),
        response_loss_old_actor.clone(),
        crate::pg_store::MetadataTransferStagingLimits::new(8, 1 << 20, 8 << 20).unwrap(),
    )
    .unwrap();
    let response_loss_intent =
        crate::pg_store::MetadataTransferStagingIntent::for_unavailable_transition(
            &evidence_request.unavailable_transition,
            evidence_request.artifact_digest,
            evidence_request.artifact_length,
            evidence_request.artifact_format_version,
        )
        .unwrap();
    response_loss_store
        .tombstone(&response_loss_intent)
        .unwrap();
    let response_loss_old_page = response_loss_store.next_evidence_page().unwrap().unwrap();
    let response_loss_old_receipt =
        crate::pg_store::MetadataTransferStagingEvidenceApplyReceipt::for_page(
            &response_loss_old_page,
        );
    let mut old_actor_snapshot = staged.snapshot().clone();
    let old_actor_node = old_actor_snapshot
        .nodes
        .get_mut(&evidence_actor_id)
        .unwrap();
    old_actor_node.node_incarnation = response_loss_old_actor.node_incarnation();
    old_actor_node.endpoint = response_loss_old_actor.endpoint().to_owned();
    let old_page_committed = old_actor_snapshot
        .apply_control_plane_command(
            ControlPlaneCommand::ApplyMetadataTransferStagingEvidencePage {
                operation_payload: response_loss_old_page.operation_payload().to_vec(),
                page_digest: response_loss_old_page.page_digest(),
            },
        )
        .unwrap();
    assert!(old_page_committed.changed());
    // Simulate loss of the apply response: the old store never records the receipt.
    drop(response_loss_store);

    let heartbeat_at_ms = old_page_committed
        .snapshot()
        .node(evidence_actor_id)
        .unwrap()
        .lease_deadline_ms()
        .unwrap_or(0)
        .max(
            old_page_committed
                .snapshot()
                .max_committed_timestamp_ms()
                .unwrap_or(0),
        )
        + 1;
    let requested_lease_duration_ms = 10_000;
    let mut reincarnation_heartbeat = heartbeat_from_snapshot(
        old_page_committed.snapshot(),
        evidence_actor_id.as_u32(),
        old_page_committed.snapshot().cluster_epoch(),
        heartbeat_at_ms,
    );
    reincarnation_heartbeat.node_incarnation = evidence_actor.node_incarnation();
    reincarnation_heartbeat.endpoint = evidence_actor.endpoint().to_owned();
    reincarnation_heartbeat.requested_lease_duration_ms = requested_lease_duration_ms;
    let actor_advanced = old_page_committed
        .snapshot()
        .apply_control_plane_command(ControlPlaneCommand::RecordNodeHeartbeat {
            heartbeat: reincarnation_heartbeat,
            heartbeat_at_ms,
            lease_deadline_ms: heartbeat_at_ms + requested_lease_duration_ms,
            lease_horizon_authority: None,
        })
        .unwrap();
    assert_eq!(
        actor_advanced
            .snapshot()
            .node(evidence_actor_id)
            .unwrap()
            .node_incarnation(),
        evidence_actor.node_incarnation()
    );

    let response_loss_reopened = crate::pg_store::MetadataTransferStagingStore::open(
        response_loss_tmp.path(),
        evidence_actor.clone(),
        crate::pg_store::MetadataTransferStagingLimits::new(8, 1 << 20, 8 << 20).unwrap(),
    )
    .unwrap();
    let response_loss_rebound_page = response_loss_reopened
        .next_evidence_page()
        .unwrap()
        .unwrap();
    assert_eq!(response_loss_rebound_page.actor(), &evidence_actor);
    assert_eq!(response_loss_rebound_page.previous_generation(), 0);
    let rebound_page_committed = actor_advanced
        .snapshot()
        .apply_control_plane_command(
            ControlPlaneCommand::ApplyMetadataTransferStagingEvidencePage {
                operation_payload: response_loss_rebound_page.operation_payload().to_vec(),
                page_digest: response_loss_rebound_page.page_digest(),
            },
        )
        .unwrap();
    assert!(rebound_page_committed.changed());
    let closure_key = (
        response_loss_old_actor.node_id(),
        response_loss_old_actor.node_incarnation(),
    );
    let closure = &rebound_page_committed
        .snapshot()
        .metadata_transfer_staging_actor_closures[&closure_key];
    assert_eq!(closure.source_actor, response_loss_old_actor);
    assert_eq!(closure.source_tip_generation, 1);
    assert_eq!(
        closure.source_tip_page_digest,
        response_loss_old_page.page_digest()
    );
    assert_eq!(closure.destination_actor, evidence_actor);
    assert_eq!(
        closure.destination_genesis_page_digest,
        response_loss_rebound_page.page_digest()
    );
    drop(response_loss_reopened);
    let final_actor = crate::pg_store::MetadataTransferStagingNodeIdentity::new(
        evidence_actor_id,
        evidence_actor.node_incarnation() + 1,
        "/tmp/response-loss-final-staging-actor.sock".to_owned(),
    )
    .unwrap();
    let final_heartbeat_at_ms = rebound_page_committed
        .snapshot()
        .node(evidence_actor_id)
        .unwrap()
        .lease_deadline_ms()
        .unwrap_or(0)
        .max(
            rebound_page_committed
                .snapshot()
                .max_committed_timestamp_ms()
                .unwrap_or(0),
        )
        + 1;
    let mut final_heartbeat = heartbeat_from_snapshot(
        rebound_page_committed.snapshot(),
        evidence_actor_id.as_u32(),
        rebound_page_committed.snapshot().cluster_epoch(),
        final_heartbeat_at_ms,
    );
    final_heartbeat.node_incarnation = final_actor.node_incarnation();
    final_heartbeat.endpoint = final_actor.endpoint().to_owned();
    final_heartbeat.requested_lease_duration_ms = requested_lease_duration_ms;
    let final_actor_advanced = rebound_page_committed
        .snapshot()
        .apply_control_plane_command(ControlPlaneCommand::RecordNodeHeartbeat {
            heartbeat: final_heartbeat,
            heartbeat_at_ms: final_heartbeat_at_ms,
            lease_deadline_ms: final_heartbeat_at_ms + requested_lease_duration_ms,
            lease_horizon_authority: None,
        })
        .unwrap();
    let final_store = crate::pg_store::MetadataTransferStagingStore::open(
        response_loss_tmp.path(),
        final_actor.clone(),
        crate::pg_store::MetadataTransferStagingLimits::new(8, 1 << 20, 8 << 20).unwrap(),
    )
    .unwrap();
    let final_rebound_page = final_store.next_evidence_page().unwrap().unwrap();
    let mut foreign_through_actor_payload = final_rebound_page.operation_payload().to_vec();
    let through_endpoint = evidence_actor.endpoint().as_bytes();
    let through_endpoint_offset = foreign_through_actor_payload
        .windows(through_endpoint.len())
        .enumerate()
        .filter_map(|(offset, candidate)| (candidate == through_endpoint).then_some(offset))
        .collect::<Vec<_>>();
    assert_eq!(through_endpoint_offset.len(), 1);
    let changed_byte = through_endpoint_offset[0] + through_endpoint.len() - 1;
    foreign_through_actor_payload[changed_byte] = b'x';
    let foreign_through_actor_digest = checksum::sha256::digest(&foreign_through_actor_payload);
    let foreign_through_actor = final_actor_advanced
        .snapshot()
        .apply_control_plane_command(
            ControlPlaneCommand::ApplyMetadataTransferStagingEvidencePage {
                operation_payload: foreign_through_actor_payload,
                page_digest: foreign_through_actor_digest,
            },
        )
        .unwrap_err();
    assert!(foreign_through_actor
        .to_string()
        .contains("through actor does not match retained chain identity"));
    let final_page_committed = final_actor_advanced
        .snapshot()
        .apply_control_plane_command(
            ControlPlaneCommand::ApplyMetadataTransferStagingEvidencePage {
                operation_payload: final_rebound_page.operation_payload().to_vec(),
                page_digest: final_rebound_page.page_digest(),
            },
        )
        .unwrap();
    let retained_first_closure = &final_page_committed
        .snapshot()
        .metadata_transfer_staging_actor_closures[&closure_key];
    assert_eq!(retained_first_closure.destination_actor, evidence_actor);
    assert_eq!(
        retained_first_closure.destination_genesis_page_digest,
        response_loss_rebound_page.page_digest()
    );
    let intermediate_closure = &final_page_committed
        .snapshot()
        .metadata_transfer_staging_actor_closures
        [&(evidence_actor.node_id(), evidence_actor.node_incarnation())];
    assert_eq!(intermediate_closure.source_actor, evidence_actor);
    assert_eq!(intermediate_closure.destination_actor, final_actor);
    assert_eq!(
        intermediate_closure.destination_genesis_page_digest,
        final_rebound_page.page_digest()
    );
    final_page_committed
        .snapshot()
        .validate_current_state_invariants()
        .unwrap();
    assert_eq!(
        parse_snapshot(&format_snapshot(final_page_committed.snapshot())).unwrap(),
        *final_page_committed.snapshot()
    );
    let closure_keys = final_page_committed
        .snapshot()
        .metadata_transfer_staging_actor_closures
        .keys()
        .copied()
        .collect::<Vec<_>>();
    assert_eq!(closure_keys.len(), 2);
    let mut retired_chain = final_page_committed.snapshot().clone();
    for (node_id, incarnation) in closure_keys {
        let command = retired_chain
            .retire_metadata_transfer_staging_actor_closure_command(node_id, incarnation)
            .unwrap();
        retired_chain = retired_chain
            .apply_control_plane_command(command)
            .unwrap()
            .into_snapshot();
    }
    for actor in [&response_loss_old_actor, &evidence_actor] {
        let last_generation = retired_chain
            .metadata_transfer_staging_evidence_pages
            .keys()
            .filter_map(|(node_id, incarnation, generation)| {
                (*node_id == actor.node_id() && *incarnation == actor.node_incarnation())
                    .then_some(*generation)
            })
            .max()
            .unwrap();
        retired_chain = retired_chain
            .checkpoint_metadata_transfer_staging_evidence_pages(
                actor.node_id(),
                actor.node_incarnation(),
                1,
                last_generation,
            )
            .unwrap()
            .into_snapshot();
    }
    assert_eq!(
        retired_chain
            .metadata_transfer_staging_evidence_pages
            .keys()
            .copied()
            .collect::<Vec<_>>(),
        vec![(final_actor.node_id(), final_actor.node_incarnation(), 1)]
    );
    let reconstructed_closures = retired_chain
        .reconstruct_metadata_transfer_staging_actor_closures()
        .unwrap();
    for (key, reconstructed) in &reconstructed_closures {
        assert_eq!(
            retired_chain
                .metadata_transfer_staging_actor_closures
                .get(key),
            Some(reconstructed),
            "compacted closure {key:?} changed during reconstruction"
        );
    }
    retired_chain.validate_current_state_invariants().unwrap();
    assert_eq!(
        parse_snapshot(&format_snapshot(&retired_chain)).unwrap(),
        retired_chain
    );
    let old_page_replay = rebound_page_committed
        .snapshot()
        .apply_control_plane_command(
            ControlPlaneCommand::ApplyMetadataTransferStagingEvidencePage {
                operation_payload: response_loss_old_page.operation_payload().to_vec(),
                page_digest: response_loss_old_page.page_digest(),
            },
        )
        .unwrap();
    assert!(!old_page_replay.changed());
    assert!(matches!(
        old_page_replay.response(),
        ControlPlaneCommandResponse::ApplyMetadataTransferStagingEvidencePage {
            apply_receipt
        } if apply_receipt == response_loss_old_receipt.as_bytes()
    ));
    let checkpointed_old_tip = rebound_page_committed
        .snapshot()
        .apply_control_plane_command(
            ControlPlaneCommand::CheckpointMetadataTransferStagingEvidencePages {
                actor_node_id: response_loss_old_actor.node_id(),
                actor_node_incarnation: response_loss_old_actor.node_incarnation(),
                first_generation: 1,
                last_generation: 1,
            },
        )
        .unwrap();
    assert!(checkpointed_old_tip.changed());
    assert!(!checkpointed_old_tip
        .snapshot()
        .metadata_transfer_staging_evidence_pages
        .contains_key(&(
            response_loss_old_actor.node_id(),
            response_loss_old_actor.node_incarnation(),
            1,
        )));
    assert!(checkpointed_old_tip
        .snapshot()
        .metadata_transfer_staging_evidence_checkpoint_segments
        .contains_key(&(
            response_loss_old_actor.node_id(),
            response_loss_old_actor.node_incarnation(),
            1,
        )));
    checkpointed_old_tip
        .snapshot()
        .validate_current_state_invariants()
        .unwrap();
    assert_eq!(
        parse_snapshot(&format_snapshot(checkpointed_old_tip.snapshot())).unwrap(),
        *checkpointed_old_tip.snapshot()
    );

    let mut forged_closure = rebound_page_committed.snapshot().clone();
    forged_closure
        .metadata_transfer_staging_actor_closures
        .get_mut(&closure_key)
        .unwrap()
        .rebound_evidence_digest[0] ^= 1;
    assert!(parse_snapshot(&format_snapshot(&forged_closure))
        .unwrap_err()
        .to_string()
        .contains("actor closures do not match retained chain evidence"));
    for actor in [&response_loss_old_actor, &evidence_actor] {
        assert!(rebound_page_committed
            .snapshot()
            .metadata_transfer_staging_evidence_pages
            .contains_key(&(actor.node_id(), actor.node_incarnation(), 1)));
        assert!(rebound_page_committed
            .snapshot()
            .metadata_transfer_staging_evidence
            .contains_key(&MetadataTransferStagingEvidenceKey {
                pg_id: response_loss_intent.pg_id(),
                staging_generation: response_loss_intent.staging_generation(),
                actor_node_id: actor.node_id(),
                actor_node_incarnation: actor.node_incarnation(),
                kind: crate::pg_store::MetadataTransferStagingEvidenceKind::Tombstone,
                target_epoch: None,
            }));
    }
    rebound_page_committed
        .snapshot()
        .validate_current_state_invariants()
        .unwrap();
    assert_eq!(
        parse_snapshot(&format_snapshot(rebound_page_committed.snapshot())).unwrap(),
        *rebound_page_committed.snapshot()
    );
    let evidence_page = crate::pg_store::metadata_transfer_staging_evidence_page_for_test(
        evidence_actor.clone(),
        &evidence_request.unavailable_transition,
        evidence_request.artifact_digest,
        evidence_request.artifact_length,
        evidence_request.artifact_format_version,
        crate::pg_store::MetadataTransferStagingEvidenceKind::Publication,
        None,
    );
    let duplicate_member_intent =
        crate::pg_store::MetadataTransferStagingIntent::for_unavailable_transition(
            &evidence_request.unavailable_transition,
            evidence_request.artifact_digest,
            evidence_request.artifact_length,
            evidence_request.artifact_format_version,
        )
        .unwrap();
    let duplicate_member_page =
        crate::pg_store::metadata_transfer_staging_evidence_page_with_duplicate_member_for_test(
            evidence_actor.clone(),
            &duplicate_member_intent,
            crate::pg_store::MetadataTransferStagingEvidenceKind::Publication,
            None,
        );
    let duplicate_member_command = ControlPlaneCommand::ApplyMetadataTransferStagingEvidencePage {
        operation_payload: duplicate_member_page.operation_payload().to_vec(),
        page_digest: duplicate_member_page.page_digest(),
    };
    assert!(matches!(
        staged
            .snapshot()
            .apply_control_plane_command(duplicate_member_command.clone()),
        Err(ControlPlaneError::CommandDecode { message })
            if message.contains("identity is duplicated or already retained")
    ));
    let mut replicated =
        ReplicatedControlPlaneStateMachine::new(staged.snapshot().clone(), None).unwrap();
    let committed_duplicate = replicated
        .apply_committed_command(
            ControlPlaneLogId::new(1, 1).unwrap(),
            duplicate_member_command,
        )
        .unwrap();
    assert!(matches!(
        committed_duplicate.rejection(),
        Some(ControlPlaneError::CommandDecode { message })
            if message.contains("identity is duplicated or already retained")
    ));
    assert_eq!(replicated.snapshot(), staged.snapshot());
    let evidence_command = ControlPlaneCommand::ApplyMetadataTransferStagingEvidencePage {
        operation_payload: evidence_page.operation_payload().to_vec(),
        page_digest: evidence_page.page_digest(),
    };
    let evidence_applied = staged
        .snapshot()
        .apply_control_plane_command(evidence_command.clone())
        .unwrap();
    assert!(evidence_applied.changed());
    assert_eq!(
        evidence_applied.snapshot().cluster_epoch(),
        staged.snapshot().cluster_epoch(),
        "staging evidence application must be epoch-neutral"
    );
    let ControlPlaneCommandResponse::ApplyMetadataTransferStagingEvidencePage { apply_receipt } =
        evidence_applied.response()
    else {
        panic!("staging evidence returned the wrong response kind");
    };
    let apply_receipt =
        crate::pg_store::decode_staging_evidence_apply_receipt(apply_receipt).unwrap();
    let retained_duplicate_page = crate::pg_store::metadata_transfer_staging_evidence_page_for_test(
        evidence_actor.clone(),
        &evidence_request.unavailable_transition,
        evidence_request.artifact_digest,
        evidence_request.artifact_length,
        evidence_request.artifact_format_version,
        crate::pg_store::MetadataTransferStagingEvidenceKind::Publication,
        Some(&apply_receipt),
    );
    assert!(matches!(
        evidence_applied
            .snapshot()
            .apply_control_plane_command(
                ControlPlaneCommand::ApplyMetadataTransferStagingEvidencePage {
                    operation_payload: retained_duplicate_page.operation_payload().to_vec(),
                    page_digest: retained_duplicate_page.page_digest(),
                }
            ),
        Err(ControlPlaneError::CommandDecode { message })
            if message.contains("identity is duplicated or already retained")
    ));
    let evidence_replay = evidence_applied
        .snapshot()
        .apply_control_plane_command(evidence_command.clone())
        .unwrap();
    assert!(!evidence_replay.changed());
    assert_eq!(evidence_replay.response(), evidence_applied.response());
    assert_eq!(
        parse_snapshot(&format_snapshot(evidence_applied.snapshot())).unwrap(),
        *evidence_applied.snapshot()
    );

    let same_actor_unpaged_request = &staging_requests[1];
    let same_actor_unpaged = crate::pg_store::metadata_transfer_staging_evidence_page_for_test(
        evidence_actor.clone(),
        &same_actor_unpaged_request.unavailable_transition,
        same_actor_unpaged_request.artifact_digest,
        same_actor_unpaged_request.artifact_length,
        same_actor_unpaged_request.artifact_format_version,
        crate::pg_store::MetadataTransferStagingEvidenceKind::Publication,
        None,
    );
    let same_actor_unpaged_evidence =
        crate::pg_store::decode_staging_evidence(same_actor_unpaged.entries()[0].evidence())
            .unwrap();
    let mut same_actor_unpaged_snapshot = evidence_applied.snapshot().clone();
    same_actor_unpaged_snapshot
        .metadata_transfer_staging_evidence
        .insert(
            MetadataTransferStagingEvidenceKey {
                pg_id: same_actor_unpaged_evidence.intent().pg_id(),
                staging_generation: same_actor_unpaged_evidence.intent().staging_generation(),
                actor_node_id: evidence_actor_id,
                actor_node_incarnation: evidence_actor.node_incarnation(),
                kind: same_actor_unpaged_evidence.kind(),
                target_epoch: same_actor_unpaged_evidence.target_epoch(),
            },
            same_actor_unpaged_evidence.as_bytes().to_vec(),
        );
    let same_actor_unpaged_error = parse_snapshot(&format_snapshot(&same_actor_unpaged_snapshot))
        .unwrap_err()
        .to_string();
    assert!(
        same_actor_unpaged_error.contains("does not match its retained chain commitment"),
        "unexpected unpaged-evidence error: {same_actor_unpaged_error}"
    );

    let mut future_actor_snapshot = evidence_applied.snapshot().clone();
    future_actor_snapshot
        .nodes
        .get_mut(&evidence_actor_id)
        .unwrap()
        .node_incarnation = evidence_actor.node_incarnation() - 1;
    assert!(parse_snapshot(&format_snapshot(&future_actor_snapshot))
        .unwrap_err()
        .to_string()
        .contains("page actor is incompatible with current node identity"));

    let mut same_incarnation_endpoint_snapshot = evidence_applied.snapshot().clone();
    same_incarnation_endpoint_snapshot
        .nodes
        .get_mut(&evidence_actor_id)
        .unwrap()
        .endpoint = "/tmp/forged-same-incarnation-endpoint.sock".to_owned();
    assert!(
        parse_snapshot(&format_snapshot(&same_incarnation_endpoint_snapshot))
            .unwrap_err()
            .to_string()
            .contains("page actor is incompatible with current node identity")
    );

    let second_actor_id = evidence_request
        .unavailable_transition
        .destination_acting_set()
        .iter()
        .copied()
        .find(|node_id| *node_id != evidence_actor_id)
        .unwrap();
    let second_actor_record = staged.snapshot().node(second_actor_id).unwrap();
    let second_actor = crate::pg_store::MetadataTransferStagingNodeIdentity::new(
        second_actor_id,
        second_actor_record.node_incarnation(),
        second_actor_record.endpoint().to_owned(),
    )
    .unwrap();
    let foreign_intent =
        crate::pg_store::MetadataTransferStagingIntent::for_unavailable_transition(
            &evidence_request.unavailable_transition,
            evidence_request.artifact_digest,
            evidence_request.artifact_length,
            evidence_request.artifact_format_version,
        )
        .unwrap();
    let foreign_member_page =
        crate::pg_store::metadata_transfer_staging_evidence_page_with_member_actor_for_test(
            evidence_actor.clone(),
            second_actor.clone(),
            &foreign_intent,
            crate::pg_store::MetadataTransferStagingEvidenceKind::Publication,
            None,
        );
    let foreign_evidence = foreign_member_page.entries()[0].evidence().to_vec();
    let mut foreign_member_snapshot = evidence_applied.snapshot().clone();
    foreign_member_snapshot
        .metadata_transfer_staging_evidence_pages
        .insert(
            (
                evidence_actor.node_id(),
                evidence_actor.node_incarnation(),
                foreign_member_page.generation(),
            ),
            MetadataTransferStagingEvidencePageRecord {
                operation_payload: foreign_member_page.operation_payload().to_vec(),
                page_digest: foreign_member_page.page_digest(),
                apply_receipt:
                    crate::pg_store::MetadataTransferStagingEvidenceApplyReceipt::for_page(
                        &foreign_member_page,
                    )
                    .as_bytes()
                    .to_vec(),
            },
        );
    foreign_member_snapshot
        .metadata_transfer_staging_evidence
        .insert(
            MetadataTransferStagingEvidenceKey {
                pg_id: evidence_request.unavailable_transition.pg_id(),
                staging_generation: evidence_request.staging_generation,
                actor_node_id: second_actor_id,
                actor_node_incarnation: second_actor.node_incarnation(),
                kind: crate::pg_store::MetadataTransferStagingEvidenceKind::Publication,
                target_epoch: crate::ClusterEpoch::new(
                    evidence_request
                        .unavailable_transition
                        .transition_epoch()
                        .get()
                        + 1,
                ),
            },
            foreign_evidence.clone(),
        );
    assert!(parse_snapshot(&format_snapshot(&foreign_member_snapshot))
        .unwrap_err()
        .to_string()
        .contains("page contains foreign actor evidence"));

    let mut orphan_actor_snapshot = evidence_applied.snapshot().clone();
    orphan_actor_snapshot
        .metadata_transfer_staging_evidence
        .insert(
            MetadataTransferStagingEvidenceKey {
                pg_id: evidence_request.unavailable_transition.pg_id(),
                staging_generation: evidence_request.staging_generation,
                actor_node_id: second_actor_id,
                actor_node_incarnation: second_actor.node_incarnation(),
                kind: crate::pg_store::MetadataTransferStagingEvidenceKind::Publication,
                target_epoch: crate::ClusterEpoch::new(
                    evidence_request
                        .unavailable_transition
                        .transition_epoch()
                        .get()
                        + 1,
                ),
            },
            foreign_evidence,
        );
    assert!(parse_snapshot(&format_snapshot(&orphan_actor_snapshot))
        .unwrap_err()
        .to_string()
        .contains("does not match its retained chain commitment"));

    let mut changed_actor_snapshot = evidence_applied.snapshot().clone();
    let changed_endpoint = "/tmp/reincarnated-evidence-actor.sock".to_owned();
    changed_actor_snapshot
        .nodes
        .get_mut(&evidence_actor_id)
        .unwrap()
        .endpoint = changed_endpoint.clone();
    let changed_actor_page = crate::pg_store::metadata_transfer_staging_evidence_page_for_test(
        crate::pg_store::MetadataTransferStagingNodeIdentity::new(
            evidence_actor_id,
            evidence_actor.node_incarnation(),
            changed_endpoint,
        )
        .unwrap(),
        &evidence_request.unavailable_transition,
        evidence_request.artifact_digest,
        evidence_request.artifact_length,
        evidence_request.artifact_format_version,
        crate::pg_store::MetadataTransferStagingEvidenceKind::Tombstone,
        Some(&apply_receipt),
    );
    assert!(changed_actor_snapshot
        .apply_control_plane_command(
            ControlPlaneCommand::ApplyMetadataTransferStagingEvidencePage {
                operation_payload: changed_actor_page.operation_payload().to_vec(),
                page_digest: changed_actor_page.page_digest(),
            }
        )
        .unwrap_err()
        .to_string()
        .contains("does not extend the retained generation"));

    let successor_page = crate::pg_store::metadata_transfer_staging_evidence_page_for_test(
        evidence_actor.clone(),
        &evidence_request.unavailable_transition,
        evidence_request.artifact_digest,
        evidence_request.artifact_length,
        evidence_request.artifact_format_version,
        crate::pg_store::MetadataTransferStagingEvidenceKind::Tombstone,
        Some(&apply_receipt),
    );
    let successor_applied = evidence_applied
        .snapshot()
        .apply_control_plane_command(
            ControlPlaneCommand::ApplyMetadataTransferStagingEvidencePage {
                operation_payload: successor_page.operation_payload().to_vec(),
                page_digest: successor_page.page_digest(),
            },
        )
        .unwrap();
    assert!(successor_applied.changed());
    assert_eq!(
        parse_snapshot(&format_snapshot(successor_applied.snapshot())).unwrap(),
        *successor_applied.snapshot()
    );
    let predecessor_replay = successor_applied
        .snapshot()
        .apply_control_plane_command(evidence_command.clone())
        .unwrap();
    assert!(!predecessor_replay.changed());
    assert_eq!(predecessor_replay.response(), evidence_applied.response());
    let mut missing_predecessor_page = successor_applied.snapshot().clone();
    missing_predecessor_page
        .metadata_transfer_staging_evidence_pages
        .remove(&(
            evidence_actor_id,
            evidence_actor.node_incarnation(),
            evidence_page.generation(),
        ));
    assert!(parse_snapshot(&format_snapshot(&missing_predecessor_page))
        .unwrap_err()
        .to_string()
        .contains("actor chain has a gap or invalid predecessor"));
    let mut missing_member = successor_applied.snapshot().clone();
    missing_member
        .metadata_transfer_staging_evidence
        .remove(&MetadataTransferStagingEvidenceKey {
            pg_id: evidence_request.unavailable_transition.pg_id(),
            staging_generation: evidence_request.staging_generation,
            actor_node_id: evidence_actor_id,
            actor_node_incarnation: evidence_actor.node_incarnation(),
            kind: crate::pg_store::MetadataTransferStagingEvidenceKind::Tombstone,
            target_epoch: None,
        });
    let missing_member_error = missing_member
        .validate_current_state_invariants()
        .unwrap_err();
    assert!(
        missing_member_error.contains("page member lacks exact detailed evidence"),
        "unexpected missing-member error: {missing_member_error}"
    );

    let unauthorized_page = crate::pg_store::metadata_transfer_staging_evidence_page_for_test(
        crate::pg_store::MetadataTransferStagingNodeIdentity::new(
            NodeId::new(1),
            staged
                .snapshot()
                .node(NodeId::new(1))
                .unwrap()
                .node_incarnation(),
            staged
                .snapshot()
                .node(NodeId::new(1))
                .unwrap()
                .endpoint()
                .to_owned(),
        )
        .unwrap(),
        &evidence_request.unavailable_transition,
        evidence_request.artifact_digest,
        evidence_request.artifact_length,
        evidence_request.artifact_format_version,
        crate::pg_store::MetadataTransferStagingEvidenceKind::Publication,
        None,
    );
    assert!(staged
        .snapshot()
        .apply_control_plane_command(
            ControlPlaneCommand::ApplyMetadataTransferStagingEvidencePage {
                operation_payload: unauthorized_page.operation_payload().to_vec(),
                page_digest: unauthorized_page.page_digest(),
            }
        )
        .unwrap_err()
        .to_string()
        .contains("does not match its authorization"));

    let later_epoch = staged
        .snapshot()
        .apply_control_plane_command(ControlPlaneCommand::SetPgActingSet {
            pg_id: PgId::new(99),
            acting_set: vec![NodeId::new(2), NodeId::new(3), NodeId::new(4)],
        })
        .unwrap();
    assert!(later_epoch.changed());
    let later_epoch_replay = later_epoch
        .snapshot()
        .apply_control_plane_command(staging_command.clone())
        .unwrap();
    assert!(!later_epoch_replay.changed());
    assert_eq!(later_epoch_replay.snapshot(), later_epoch.snapshot());

    let subset_error = staged
        .snapshot()
        .apply_control_plane_command(ControlPlaneCommand::AuthorizeUnavailablePgStagingIntents {
            authorizations: vec![staging_requests[0].clone()],
        })
        .unwrap_err();
    assert!(
        subset_error
            .to_string()
            .contains("conflicts with durable artifact identity"),
        "unexpected staging subset replay error: {subset_error}"
    );
    let mut divergent_requests = staging_requests.clone();
    divergent_requests[1].artifact_digest[0] ^= 1;
    assert!(recorded
        .apply_control_plane_command(ControlPlaneCommand::AuthorizeUnavailablePgStagingIntents {
            authorizations: divergent_requests,
        })
        .is_ok());
    assert_eq!(
        recorded
            .unavailable_pg_placement_transition(pg_ids[0])
            .unwrap()
            .staging_authorization,
        None,
        "validation must not mutate its source snapshot"
    );
    let mut invalid_requests = staging_requests.clone();
    invalid_requests[1].staging_generation += 1;
    assert!(recorded
        .apply_control_plane_command(ControlPlaneCommand::AuthorizeUnavailablePgStagingIntents {
            authorizations: invalid_requests,
        })
        .unwrap_err()
        .to_string()
        .contains("invalid generation"));
    let mut forged = staged.snapshot().clone();
    forged
        .unavailable_pg_placement_transitions
        .get_mut(&pg_ids[0])
        .unwrap()
        .staging_authorization
        .as_mut()
        .unwrap()
        .artifact_digest[0] ^= 1;
    assert!(forged
        .validate_current_state_invariants()
        .unwrap_err()
        .to_string()
        .contains("receipt digest"));
    let mut future_receipt = staged.snapshot().clone();
    let future_epoch = next_epoch(future_receipt.cluster_epoch()).unwrap();
    let authorization = future_receipt
        .unavailable_pg_placement_transitions
        .get_mut(&pg_ids[0])
        .unwrap()
        .staging_authorization
        .as_mut()
        .unwrap();
    authorization.batch_receipt.source_epoch = future_epoch;
    authorization.batch_receipt.target_epoch = future_epoch;
    assert!(future_receipt
        .validate_current_state_invariants()
        .unwrap_err()
        .to_string()
        .contains("invalid staging authorization"));
    let mut oversized_snapshot = recorded.clone();
    let transition_epochs = pg_ids
        .iter()
        .copied()
        .map(|pg_id| {
            (
                pg_id,
                oversized_snapshot
                    .unavailable_pg_placement_transition(pg_id)
                    .unwrap()
                    .transition_epoch(),
            )
        })
        .collect::<Vec<_>>();
    let oversized_receipt_epoch = oversized_snapshot.cluster_epoch();
    forge_staging_authorization_at_epoch_with_length(
        &mut oversized_snapshot,
        &transition_epochs,
        oversized_receipt_epoch,
        |_| crate::pg_store::METADATA_TRANSFER_STAGED_ARTIFACT_MAX_BYTES + 1,
    );
    assert!(oversized_snapshot
        .validate_current_state_invariants()
        .unwrap_err()
        .to_string()
        .contains("invalid staging authorization"));

    let authorized = ControlPlaneLinearizedCommandSink::submit_control_plane_command(
        &mut authority,
        staging_command.clone(),
    )
    .unwrap();
    assert!(authorized.changed());

    let destination_epoch = next_epoch(authority.snapshot().cluster_epoch()).unwrap();
    let mut installs = Vec::new();
    for (pg_id, authorization) in pg_ids.into_iter().zip(&staging_requests) {
        let transition = authority
            .snapshot()
            .unavailable_pg_placement_transition(pg_id)
            .unwrap();
        let work = UnavailablePgReconciliationWork::from_transition(
            transition,
            UnavailablePgReconciliationStage::MetadataTransfer,
        );
        let transfer = PgMetadataTransferProof::new(work.source_epoch(), active_proof);
        let intent = crate::pg_store::MetadataTransferStagingIntent::for_unavailable_transition(
            work.mutation_binding(),
            authorization.artifact_digest,
            authorization.artifact_length,
            authorization.artifact_format_version,
        )
        .unwrap();
        let mut publications = Vec::new();
        for node_id in work.destination_acting_set().iter().copied() {
            let node = authority.snapshot().node(node_id).unwrap();
            let previous = latest_staging_evidence_apply_receipt(
                authority.snapshot(),
                node_id,
                node.node_incarnation(),
            );
            let page = crate::pg_store::metadata_transfer_staging_publication_evidence_page_at_epoch_for_test(
                crate::pg_store::MetadataTransferStagingNodeIdentity::new(
                    node_id,
                    node.node_incarnation(),
                    node.endpoint().to_owned(),
                )
                .unwrap(),
                &intent,
                destination_epoch,
                transfer,
                previous.as_ref(),
            );
            ControlPlaneAdmin::apply_metadata_transfer_staging_evidence_page(
                &mut authority,
                page.operation_payload().to_vec(),
                page.page_digest(),
            )
            .unwrap();
            let node = authority.snapshot().node(node_id).unwrap();
            let key = MetadataTransferStagingEvidenceKey {
                pg_id,
                staging_generation: authorization.staging_generation,
                actor_node_id: node_id,
                actor_node_incarnation: node.node_incarnation(),
                kind: crate::pg_store::MetadataTransferStagingEvidenceKind::Publication,
                target_epoch: Some(destination_epoch),
            };
            publications.push(UnavailablePgStagingPublicationBinding {
                node_id,
                node_incarnation: node.node_incarnation(),
                endpoint: node.endpoint().to_owned(),
                evidence_digest: checksum::sha256::digest(
                    &authority.snapshot().metadata_transfer_staging_evidence[&key],
                ),
            });
        }
        publications.sort_by_key(|publication| publication.node_id);
        installs.push(UnavailablePgTransitionInstallRequest {
            unavailable_transition: work.mutation_binding().clone(),
            transfer,
            expected_destination_epoch: destination_epoch,
            publications,
        });
    }
    installs.sort_by_key(|install| install.unavailable_transition.pg_id());
    authority
        .install_unavailable_pg_placement_transitions_batch(&installs, destination_epoch)
        .unwrap();
    let installed_authorized = authority.snapshot().clone();
    installed_authorized
        .validate_current_state_invariants()
        .unwrap();
    assert_eq!(
        parse_snapshot(&format_snapshot(&installed_authorized)).unwrap(),
        installed_authorized
    );
    assert!(pg_ids.iter().all(|pg_id| authority
        .snapshot()
        .unavailable_pg_placement_transition(*pg_id)
        .unwrap()
        .staging_authorization
        .is_some()));
    let mut post_install_forgery = authority.snapshot().clone();
    let forged_receipt_epoch = post_install_forgery.cluster_epoch();
    let installed_transitions = pg_ids
        .iter()
        .copied()
        .map(|pg_id| {
            (
                pg_id,
                post_install_forgery
                    .unavailable_pg_placement_transition(pg_id)
                    .unwrap()
                    .transition_epoch(),
            )
        })
        .collect::<Vec<_>>();
    forge_staging_authorization_at_epoch(
        &mut post_install_forgery,
        &installed_transitions,
        forged_receipt_epoch,
    );
    let post_install_error = parse_snapshot(&format_snapshot(&post_install_forgery)).unwrap_err();
    assert!(
        post_install_error
            .to_string()
            .contains("staging evidence does not match its authorization"),
        "unexpected post-install staging authorization error: {post_install_error}"
    );
    let post_install_replay = authority
        .snapshot()
        .apply_control_plane_command(ControlPlaneCommand::AuthorizeUnavailablePgStagingIntents {
            authorizations: staging_requests.clone(),
        })
        .unwrap();
    assert!(!post_install_replay.changed());
    assert_eq!(post_install_replay.snapshot(), authority.snapshot());
    let readiness_at_ms = authority.snapshot().max_committed_timestamp_ms().unwrap() + 1;
    for node_id in [4, 2, 3] {
        heartbeat_with_pg_proofs_and_lease_duration(
            &mut authority,
            node_id,
            &pg_ids,
            PgState::Peering,
            active_proof,
            readiness_at_ms + u64::from(node_id),
            10_000,
        );
    }
    let ready_at_ms = (authority.snapshot().max_committed_timestamp_ms().unwrap() + 1)
        .max(failed_deadline + CONTROL_PLANE_CLOCK_SKEW_BUDGET_MS + 1);
    let completion_work = pg_ids
        .into_iter()
        .map(|pg_id| {
            UnavailablePgReconciliationWork::from_transition(
                authority
                    .snapshot()
                    .unavailable_pg_placement_transition(pg_id)
                    .unwrap(),
                UnavailablePgReconciliationStage::PayloadReadiness,
            )
        })
        .collect::<Vec<_>>();
    let stale_second = UnavailablePgReconciliationWork::new(
        completion_work[1].pg_id(),
        completion_work[1].source_epoch(),
        completion_work[1].source_epoch(),
        completion_work[1].source_acting_set().to_vec(),
        completion_work[1].destination_acting_set().to_vec(),
        UnavailablePgReconciliationStage::PayloadReadiness,
    );
    let prepared_with_stale_member = authority
        .snapshot()
        .prepare_unavailable_pg_placement_completion_batch(
            &[completion_work[0].clone(), stale_second],
            ready_at_ms,
        )
        .unwrap();
    assert_eq!(
        prepared_with_stale_member
            .included
            .iter()
            .map(UnavailablePgReconciliationWork::pg_id)
            .collect::<Vec<_>>(),
        vec![completion_work[0].pg_id()]
    );
    assert_eq!(prepared_with_stale_member.rejected.len(), 1);

    let full_completion = authority
        .snapshot()
        .complete_unavailable_pg_placement_transition_batch_command(&completion_work, ready_at_ms)
        .unwrap();
    let singleton_completion = authority
        .snapshot()
        .complete_unavailable_pg_placement_transition_batch_command(
            std::slice::from_ref(&completion_work[0]),
            ready_at_ms,
        )
        .unwrap();
    let full_len =
        crate::control_plane_raft::control_plane_command_replication_encoded_len(&full_completion)
            .unwrap();
    let singleton_len = crate::control_plane_raft::control_plane_command_replication_encoded_len(
        &singleton_completion,
    )
    .unwrap();
    assert!(singleton_len < full_len);
    let split_limit = singleton_len + (full_len - singleton_len) / 2;
    let split_completion = authority
        .snapshot()
        .prepare_unavailable_pg_placement_completion_batch_with_replication_limit(
            &completion_work,
            ready_at_ms,
            split_limit,
        )
        .unwrap();
    assert_eq!(split_completion.included.len(), 1);
    assert!(split_completion.rejected.is_empty());
    assert!(
        crate::control_plane_raft::control_plane_command_replication_encoded_len(
            split_completion.command.as_ref().unwrap()
        )
        .unwrap()
            <= split_limit
    );
    let completion_command = authority
        .snapshot()
        .complete_unavailable_pg_placement_transition_batch_command(&completion_work, ready_at_ms)
        .unwrap();
    let ControlPlaneCommand::CompleteUnavailablePgPlacementTransitions {
        ready_at_ms: completion_ready_at_ms,
        transitions: completion_requests,
    } = completion_command.clone()
    else {
        unreachable!("completion batch builder returned the wrong command kind");
    };
    assert_eq!(completion_ready_at_ms, ready_at_ms);
    assert_eq!(completion_requests.len(), 2);

    let mut invalid_completion = completion_requests.clone();
    invalid_completion[1].completion.pg_id = pg_ids[0];
    let before_invalid_completion = authority.snapshot().clone();
    assert!(authority
        .snapshot()
        .apply_control_plane_command(
            ControlPlaneCommand::CompleteUnavailablePgPlacementTransitions {
                ready_at_ms,
                transitions: invalid_completion,
            },
        )
        .is_err());
    assert_eq!(authority.snapshot(), &before_invalid_completion);

    authority
        .complete_unavailable_pg_placement_transition_batch(&completion_work, ready_at_ms)
        .unwrap();
    let completed = authority.snapshot().clone();
    completed.validate_current_state_invariants().unwrap();
    assert_eq!(
        parse_snapshot(&format_snapshot(&completed)).unwrap(),
        completed
    );
    for pg_id in pg_ids {
        assert_eq!(completed.pg(pg_id).unwrap().state(), PgState::Active);
        assert!(completed
            .unavailable_pg_placement_transition(pg_id)
            .is_none());
        assert!(completed
            .retained_unavailable_pg_placement_transitions()
            .find(|transition| transition.pg_id() == pg_id)
            .unwrap()
            .staging_authorization
            .is_some());
    }

    let exact_completion_replay = completed
        .apply_control_plane_command(completion_command)
        .unwrap();
    assert!(!exact_completion_replay.changed());
    assert_eq!(exact_completion_replay.snapshot(), &completed);

    let subset_completion = completed
        .apply_control_plane_command(
            ControlPlaneCommand::CompleteUnavailablePgPlacementTransitions {
                ready_at_ms,
                transitions: vec![completion_requests[0].clone()],
            },
        )
        .unwrap_err();
    assert!(
        subset_completion
            .to_string()
            .contains("no matching active unavailable placement transition"),
        "unexpected completion subset replay error: {subset_completion}"
    );

    let mut reordered_completion = completion_requests;
    reordered_completion.reverse();
    assert!(completed
        .apply_control_plane_command(
            ControlPlaneCommand::CompleteUnavailablePgPlacementTransitions {
                ready_at_ms,
                transitions: reordered_completion,
            },
        )
        .unwrap_err()
        .to_string()
        .contains("strictly increasing"));
}

#[test]
fn actor_chain_closure_waits_for_the_complete_rebound_page_prefix() {
    let (_tmp, _store, authority, _) = certified_spare_authority();
    let mut snapshot = authority.snapshot().clone();
    let actor_node_id = NodeId::new(4);
    let old_actor = crate::pg_store::MetadataTransferStagingNodeIdentity::new(
        actor_node_id,
        7,
        "unix:///tmp/staging-actor-7.sock".to_owned(),
    )
    .unwrap();
    let next_actor = crate::pg_store::MetadataTransferStagingNodeIdentity::new(
        actor_node_id,
        8,
        "/tmp/transition-node-4.sock".to_owned(),
    )
    .unwrap();
    let node = snapshot.nodes.get_mut(&actor_node_id).unwrap();
    node.node_incarnation = next_actor.node_incarnation();
    node.endpoint = next_actor.endpoint().to_owned();

    let staging_tmp = test_util::tempdir();
    let evidence_count = crate::pg_store::MAX_STAGING_EVIDENCE_PAGE_ENTRIES + 1;
    let limits = crate::pg_store::MetadataTransferStagingLimits::new(
        evidence_count,
        1024,
        u64::try_from(evidence_count).unwrap() * 1024,
    )
    .unwrap();
    let old_store = crate::pg_store::MetadataTransferStagingStore::open(
        staging_tmp.path(),
        old_actor.clone(),
        limits,
    )
    .unwrap();
    for pg in 1..=u32::try_from(evidence_count).unwrap() {
        let binding = UnavailablePgTransitionMutationBinding::new(
            PgId::new(pg),
            ClusterEpoch::new(9).unwrap(),
            ClusterEpoch::new(7).unwrap(),
            vec![NodeId::new(1), NodeId::new(2), NodeId::new(3)],
            vec![NodeId::new(1), actor_node_id, NodeId::new(3)],
        );
        let intent = crate::pg_store::MetadataTransferStagingIntent::for_unavailable_transition(
            &binding,
            [u8::try_from(pg).unwrap(); 32],
            1,
            crate::pg_store::METADATA_TRANSFER_STAGED_ARTIFACT_FORMAT_VERSION,
        )
        .unwrap();
        old_store.tombstone(&intent).unwrap();
    }
    let old_page = old_store.next_evidence_page().unwrap().unwrap();
    drop(old_store);

    let rebound_store = crate::pg_store::MetadataTransferStagingStore::open(
        staging_tmp.path(),
        next_actor.clone(),
        limits,
    )
    .unwrap();
    let first_rebound_page = rebound_store.next_evidence_page().unwrap().unwrap();
    assert!(first_rebound_page.entries().len() < evidence_count);
    retain_staging_evidence_page_for_test(&mut snapshot, &old_page);
    retain_staging_evidence_page_for_test(&mut snapshot, &first_rebound_page);
    snapshot.metadata_transfer_staging_actor_closures = snapshot
        .reconstruct_metadata_transfer_staging_actor_closures()
        .unwrap();
    assert!(snapshot.metadata_transfer_staging_actor_closures.is_empty());

    let first_receipt =
        crate::pg_store::MetadataTransferStagingEvidenceApplyReceipt::for_page(&first_rebound_page);
    rebound_store
        .record_evidence_apply_receipt(&first_rebound_page, &first_receipt)
        .unwrap();
    let final_rebound_page = rebound_store.next_evidence_page().unwrap().unwrap();
    retain_staging_evidence_page_for_test(&mut snapshot, &final_rebound_page);
    snapshot.metadata_transfer_staging_actor_closures = snapshot
        .reconstruct_metadata_transfer_staging_actor_closures()
        .unwrap();

    let closure = &snapshot.metadata_transfer_staging_actor_closures
        [&(old_actor.node_id(), old_actor.node_incarnation())];
    assert_eq!(closure.source_actor, old_actor);
    assert_eq!(closure.source_tip_generation, old_page.generation());
    assert_eq!(closure.destination_actor, next_actor);
    assert_eq!(
        closure.rebound_entry_count,
        u64::try_from(evidence_count).unwrap()
    );

    let destination_checkpoint = snapshot
        .checkpoint_metadata_transfer_staging_evidence_pages(
            next_actor.node_id(),
            next_actor.node_incarnation(),
            1,
            1,
        )
        .unwrap_err();
    assert!(destination_checkpoint
        .to_string()
        .contains("cannot consume an actor-closure chain"));

    let compacted_source = snapshot
        .checkpoint_metadata_transfer_staging_evidence_pages(
            old_actor.node_id(),
            old_actor.node_incarnation(),
            1,
            1,
        )
        .unwrap()
        .into_snapshot();
    assert!(!compacted_source
        .metadata_transfer_staging_evidence_pages
        .contains_key(&(old_actor.node_id(), old_actor.node_incarnation(), 1)));
    assert_eq!(
        compacted_source
            .reconstruct_metadata_transfer_staging_actor_closures()
            .unwrap(),
        compacted_source.metadata_transfer_staging_actor_closures
    );
}

#[test]
fn actor_closure_retirement_is_exact_epoch_neutral_and_enables_compaction() {
    let snapshot = staging_actor_closure_snapshot_fixture();
    let (closure_key, closure) = snapshot
        .metadata_transfer_staging_actor_closures
        .iter()
        .next()
        .map(|(key, closure)| (*key, closure.clone()))
        .unwrap();
    let retirement = snapshot
        .retire_metadata_transfer_staging_actor_closure_command(closure_key.0, closure_key.1)
        .unwrap();
    let ControlPlaneCommand::RetireMetadataTransferStagingActorClosure {
        certificate_digest, ..
    } = retirement.clone()
    else {
        unreachable!("closure retirement builder returned the wrong command");
    };
    let mut wrong_digest = certificate_digest;
    wrong_digest[0] ^= 1;
    let unchanged = snapshot.clone();
    let mismatch = snapshot
        .apply_control_plane_command(
            ControlPlaneCommand::RetireMetadataTransferStagingActorClosure {
                actor_node_id: closure_key.0,
                actor_node_incarnation: closure_key.1,
                certificate_digest: wrong_digest,
            },
        )
        .unwrap_err();
    assert!(mismatch.to_string().contains("exact certificate"));
    assert_eq!(snapshot, unchanged);

    let retired = snapshot
        .apply_control_plane_command(retirement.clone())
        .unwrap();
    assert!(retired.changed());
    assert_eq!(
        retired
            .snapshot()
            .metadata_transfer_staging_retired_actor_closures
            .get(&closure_key),
        retired
            .snapshot()
            .metadata_transfer_staging_actor_closures
            .get(&closure_key)
    );
    assert_eq!(retired.snapshot().cluster_epoch(), snapshot.cluster_epoch());
    let replayed = retired
        .snapshot()
        .apply_control_plane_command(retirement)
        .unwrap();
    assert!(!replayed.changed());
    assert!(matches!(
        replayed.response(),
        ControlPlaneCommandResponse::RetireMetadataTransferStagingActorClosure
    ));

    let transition = replayed
        .snapshot()
        .unavailable_pg_placement_transitions
        .values()
        .chain(
            replayed
                .snapshot()
                .retained_unavailable_pg_placement_transitions
                .values(),
        )
        .find(|transition| {
            transition
                .destination_acting_set
                .contains(&closure.destination_actor.node_id())
        })
        .unwrap();
    let authorization = transition.staging_authorization.as_ref().unwrap();
    let binding = UnavailablePgTransitionMutationBinding::new(
        transition.pg_id,
        transition.transition_epoch,
        transition.source_epoch,
        transition.source_acting_set.clone(),
        transition.destination_acting_set.clone(),
    );
    let latest_destination_page = replayed
        .snapshot()
        .metadata_transfer_staging_evidence_pages
        .range(
            (
                closure.destination_actor.node_id(),
                closure.destination_actor.node_incarnation(),
                0,
            )
                ..=(
                    closure.destination_actor.node_id(),
                    closure.destination_actor.node_incarnation(),
                    u64::MAX,
                ),
        )
        .next_back()
        .unwrap()
        .1;
    let latest_destination_receipt = crate::pg_store::decode_staging_evidence_apply_receipt(
        &latest_destination_page.apply_receipt,
    )
    .unwrap();
    let successor_page = crate::pg_store::metadata_transfer_staging_evidence_page_for_test(
        closure.destination_actor.clone(),
        &binding,
        authorization.artifact_digest,
        authorization.artifact_length,
        authorization.artifact_format_version,
        crate::pg_store::MetadataTransferStagingEvidenceKind::Publication,
        Some(&latest_destination_receipt),
    );
    let after_successor = replayed
        .snapshot()
        .apply_control_plane_command(
            ControlPlaneCommand::ApplyMetadataTransferStagingEvidencePage {
                operation_payload: successor_page.operation_payload().to_vec(),
                page_digest: successor_page.page_digest(),
            },
        )
        .unwrap()
        .into_snapshot();
    assert_eq!(
        after_successor
            .metadata_transfer_staging_actor_closures
            .get(&closure_key),
        Some(&closure)
    );
    assert_eq!(
        after_successor
            .metadata_transfer_staging_retired_actor_closures
            .get(&closure_key),
        Some(&closure)
    );

    let compacted_source = after_successor
        .checkpoint_metadata_transfer_staging_evidence_pages(
            closure.source_actor.node_id(),
            closure.source_actor.node_incarnation(),
            1,
            1,
        )
        .unwrap()
        .into_snapshot();
    let compacted_destination = compacted_source
        .checkpoint_metadata_transfer_staging_evidence_pages(
            closure.destination_actor.node_id(),
            closure.destination_actor.node_incarnation(),
            1,
            1,
        )
        .unwrap()
        .into_snapshot();
    assert!(!compacted_destination
        .metadata_transfer_staging_evidence_pages
        .contains_key(&(
            closure.destination_actor.node_id(),
            closure.destination_actor.node_incarnation(),
            1,
        )));
    compacted_destination
        .validate_current_state_invariants()
        .unwrap();
    assert_eq!(
        parse_snapshot(&format_snapshot(&compacted_destination)).unwrap(),
        compacted_destination
    );

    let mut forged = compacted_destination.clone();
    forged
        .metadata_transfer_staging_actor_closures
        .get_mut(&closure_key)
        .unwrap()
        .rebound_evidence_digest[0] ^= 1;
    forged
        .metadata_transfer_staging_retired_actor_closures
        .get_mut(&closure_key)
        .unwrap()
        .rebound_evidence_digest[0] ^= 1;
    let error = parse_snapshot(&format_snapshot(&forged)).unwrap_err();
    assert!(
        error.to_string().contains("rebound evidence prefix"),
        "unexpected coordinated forgery error: {error}"
    );
}

#[test]
fn incomplete_actor_closure_prefix_validates_through_actor_before_acknowledgement() {
    let mut snapshot = staging_actor_closure_snapshot_fixture();
    let existing_closure = snapshot
        .metadata_transfer_staging_actor_closures
        .values()
        .next()
        .unwrap()
        .clone();
    let first_tip_record = snapshot
        .metadata_transfer_staging_evidence_pages
        .get(&(
            existing_closure.source_actor.node_id(),
            existing_closure.source_actor.node_incarnation(),
            existing_closure.source_tip_generation,
        ))
        .unwrap();
    let first_tip = crate::pg_store::decode_staging_evidence_page_payload(
        &first_tip_record.operation_payload,
        first_tip_record.page_digest,
    )
    .unwrap();
    let through_record = snapshot
        .metadata_transfer_staging_evidence_pages
        .get(&(
            existing_closure.destination_actor.node_id(),
            existing_closure.destination_actor.node_incarnation(),
            1,
        ))
        .unwrap();
    let through_page = crate::pg_store::decode_staging_evidence_page_payload(
        &through_record.operation_payload,
        through_record.page_digest,
    )
    .unwrap();
    let destination_actor = crate::pg_store::MetadataTransferStagingNodeIdentity::new(
        existing_closure.destination_actor.node_id(),
        existing_closure.destination_actor.node_incarnation() + 1,
        "/tmp/incomplete-closure-destination.sock".to_owned(),
    )
    .unwrap();
    let node = snapshot
        .nodes
        .get_mut(&destination_actor.node_id())
        .unwrap();
    node.node_incarnation = destination_actor.node_incarnation();
    node.endpoint = destination_actor.endpoint().to_owned();
    let incomplete_page =
        crate::pg_store::metadata_transfer_staging_incomplete_closure_evidence_page_for_test(
            &first_tip,
            existing_closure.destination_actor.clone(),
            destination_actor.clone(),
            &through_page.entries()[0],
        );
    assert_eq!(incomplete_page.entries().len(), 1);
    assert_eq!(
        incomplete_page
            .actor_closure_candidate()
            .unwrap()
            .rebound_entry_count(),
        2
    );

    let mut foreign_through_actor_payload = incomplete_page.operation_payload().to_vec();
    let through_endpoint = existing_closure.destination_actor.endpoint().as_bytes();
    let through_endpoint_offsets = foreign_through_actor_payload
        .windows(through_endpoint.len())
        .enumerate()
        .filter_map(|(offset, bytes)| (bytes == through_endpoint).then_some(offset))
        .collect::<Vec<_>>();
    assert_eq!(through_endpoint_offsets.len(), 1);
    let changed_byte = through_endpoint_offsets[0] + through_endpoint.len() - 1;
    foreign_through_actor_payload[changed_byte] = b'x';
    let foreign_through_actor_digest = checksum::sha256::digest(&foreign_through_actor_payload);
    let unchanged = snapshot.clone();
    let error = snapshot
        .apply_control_plane_command(
            ControlPlaneCommand::ApplyMetadataTransferStagingEvidencePage {
                operation_payload: foreign_through_actor_payload,
                page_digest: foreign_through_actor_digest,
            },
        )
        .unwrap_err();
    assert!(error
        .to_string()
        .contains("through actor does not match retained chain identity"));
    assert_eq!(snapshot, unchanged);
    assert!(!snapshot
        .metadata_transfer_staging_evidence_pages
        .contains_key(&(
            destination_actor.node_id(),
            destination_actor.node_incarnation(),
            1,
        )));

    let incomplete_applied = snapshot
        .apply_control_plane_command(
            ControlPlaneCommand::ApplyMetadataTransferStagingEvidencePage {
                operation_payload: incomplete_page.operation_payload().to_vec(),
                page_digest: incomplete_page.page_digest(),
            },
        )
        .unwrap();
    assert!(incomplete_applied.changed());
    assert!(!incomplete_applied
        .snapshot()
        .metadata_transfer_staging_actor_closures
        .contains_key(&(
            existing_closure.destination_actor.node_id(),
            existing_closure.destination_actor.node_incarnation(),
        )));
}

#[test]
fn actor_chain_closure_selects_acknowledged_tip_when_assigned_successor_was_not_dispatched() {
    let (_tmp, _store, authority, _) = certified_spare_authority();
    let mut snapshot = authority.snapshot().clone();
    let old_actor = crate::pg_store::MetadataTransferStagingNodeIdentity::new(
        NodeId::new(4),
        7,
        "unix:///tmp/staging-actor-7.sock".to_owned(),
    )
    .unwrap();
    let next_actor = crate::pg_store::MetadataTransferStagingNodeIdentity::new(
        NodeId::new(4),
        8,
        "/tmp/transition-node-4.sock".to_owned(),
    )
    .unwrap();
    let node = snapshot.nodes.get_mut(&NodeId::new(4)).unwrap();
    node.node_incarnation = next_actor.node_incarnation();
    node.endpoint = next_actor.endpoint().to_owned();

    let staging_tmp = test_util::tempdir();
    let limits = crate::pg_store::MetadataTransferStagingLimits::new(4, 1024, 4096).unwrap();
    let old_store = crate::pg_store::MetadataTransferStagingStore::open(
        staging_tmp.path(),
        old_actor.clone(),
        limits,
    )
    .unwrap();
    let intent_for_pg = |pg_id| {
        crate::pg_store::MetadataTransferStagingIntent::for_unavailable_transition(
            &UnavailablePgTransitionMutationBinding::new(
                PgId::new(pg_id),
                ClusterEpoch::new(9).unwrap(),
                ClusterEpoch::new(7).unwrap(),
                vec![NodeId::new(1), NodeId::new(2), NodeId::new(3)],
                vec![NodeId::new(1), NodeId::new(4), NodeId::new(3)],
            ),
            [u8::try_from(pg_id).unwrap(); 32],
            1,
            crate::pg_store::METADATA_TRANSFER_STAGED_ARTIFACT_FORMAT_VERSION,
        )
        .unwrap()
    };
    old_store.tombstone(&intent_for_pg(7)).unwrap();
    let acknowledged_page = old_store.next_evidence_page().unwrap().unwrap();
    let acknowledged_receipt =
        crate::pg_store::MetadataTransferStagingEvidenceApplyReceipt::for_page(&acknowledged_page);
    old_store
        .record_evidence_apply_receipt(&acknowledged_page, &acknowledged_receipt)
        .unwrap();
    old_store.tombstone(&intent_for_pg(8)).unwrap();
    let assigned_successor = old_store.next_evidence_page().unwrap().unwrap();
    assert_eq!(assigned_successor.generation(), 2);
    retain_staging_evidence_page_for_test(&mut snapshot, &acknowledged_page);
    drop(old_store);

    let rebound_store = crate::pg_store::MetadataTransferStagingStore::open(
        staging_tmp.path(),
        next_actor.clone(),
        limits,
    )
    .unwrap();
    let rebound_genesis = rebound_store.next_evidence_page().unwrap().unwrap();
    let candidate = rebound_genesis.actor_closure_candidate().unwrap();
    assert!(candidate.accepts_first_tip(
        &old_actor,
        assigned_successor.generation(),
        assigned_successor.page_digest(),
        checksum::sha256::digest(
            crate::pg_store::MetadataTransferStagingEvidenceApplyReceipt::for_page(
                &assigned_successor,
            )
            .as_bytes(),
        ),
    ));
    retain_staging_evidence_page_for_test(&mut snapshot, &rebound_genesis);
    snapshot.metadata_transfer_staging_actor_closures = snapshot
        .reconstruct_metadata_transfer_staging_actor_closures()
        .unwrap();

    let closure = &snapshot.metadata_transfer_staging_actor_closures
        [&(old_actor.node_id(), old_actor.node_incarnation())];
    assert_eq!(
        closure.source_tip_generation,
        acknowledged_page.generation()
    );
    assert_eq!(
        closure.source_tip_page_digest,
        acknowledged_page.page_digest()
    );
    assert_eq!(closure.destination_actor, next_actor);
}

#[test]
fn unavailable_pg_transition_is_exact_durable_and_uses_the_committed_spare() {
    let (_tmp, store, mut authority, pg_id) = certified_spare_authority();
    for node_id in 1..=4 {
        heartbeat_spare_node(&mut authority, node_id, 1_000 + u64::from(node_id));
    }
    let active_proof = PgMetadataProof::current(17, 0x1717, 0x2727);
    for node_id in 1..=3 {
        heartbeat_with_pg_proof_and_lease_duration(
            &mut authority,
            node_id,
            pg_id.get(),
            PgState::Peering,
            active_proof,
            false,
            (2_000 + u64::from(node_id), 10_000),
        );
    }
    authority
        .complete_pg_peering(
            pg_id,
            NodeId::new(1),
            node_incarnation(&authority, 1),
            2_004,
        )
        .unwrap();
    for node_id in 1..=3 {
        let lease_duration_ms = if node_id == 1 { 100 } else { 10_000 };
        heartbeat_with_pg_proof_and_lease_duration(
            &mut authority,
            node_id,
            pg_id.get(),
            PgState::Active,
            active_proof,
            false,
            (3_000 + u64::from(node_id), lease_duration_ms),
        );
    }
    let failed_deadline = authority
        .snapshot()
        .node(NodeId::new(1))
        .unwrap()
        .lease_deadline_ms()
        .unwrap();
    authority.expire_heartbeat_leases(failed_deadline).unwrap();
    for node_id in 2..=4 {
        heartbeat_spare_node(
            &mut authority,
            node_id,
            failed_deadline + u64::from(node_id),
        );
    }
    let survivor_refresh_at_ms = authority.snapshot().max_committed_timestamp_ms().unwrap() + 1;
    for node_id in 2..=4 {
        heartbeat_spare_node(
            &mut authority,
            node_id,
            survivor_refresh_at_ms + u64::from(node_id),
        );
    }
    let begin_at_without_pg_evidence = authority
        .snapshot()
        .unavailable_node_observation(NodeId::new(1))
        .unwrap()
        .observed_at_ms()
        + 50;
    assert!(authority
        .snapshot()
        .begin_unavailable_pg_placement_transition_command(
            pg_id,
            NodeId::new(1),
            begin_at_without_pg_evidence,
        )
        .unwrap_err()
        .to_string()
        .contains("no proof-qualified serving metadata-transfer source"));
    for node_id in [2, 3] {
        heartbeat_with_pg_proof_and_lease_duration(
            &mut authority,
            node_id,
            pg_id.get(),
            PgState::Peering,
            active_proof,
            false,
            (survivor_refresh_at_ms + 10 + u64::from(node_id), 10_000),
        );
    }
    let observation = authority
        .snapshot()
        .unavailable_node_observation(NodeId::new(1))
        .unwrap()
        .clone();
    let begin_at_ms = observation.observed_at_ms() + 50;
    for node_id in 2..=4 {
        let node = authority.snapshot().node(NodeId::new(node_id)).unwrap();
        assert!(
            node.can_serve_primary(authority.snapshot().cluster_epoch(), begin_at_ms),
            "node {node_id} should serve at {begin_at_ms}: {node:?}"
        );
    }
    let stale_proposal = authority
        .snapshot()
        .begin_unavailable_pg_placement_transition_command(pg_id, NodeId::new(1), begin_at_ms)
        .unwrap();
    let mut durable = authority.snapshot().clone();
    let renewal_at_ms = durable.max_committed_timestamp_ms().unwrap() + 1;
    let horizon_authority = LeaseHorizonAuthorityBinding::new(3, Some(9));
    durable.lease_grant_horizon = Some(CommittedLeaseGrantHorizon::from_parts(
        horizon_authority,
        renewal_at_ms + 20_000,
    ));
    let previous_observation = *durable
        .node(NodeId::new(2))
        .unwrap()
        .pg_observation(pg_id)
        .unwrap();
    let mut renewed_heartbeat =
        heartbeat_from_snapshot(&durable, 2, durable.cluster_epoch(), renewal_at_ms);
    renewed_heartbeat.requested_lease_duration_ms = 10_000;
    renewed_heartbeat.pg_observations = vec![NodePgHeartbeatObservation {
        pg_id,
        state: previous_observation.state(),
        metadata_proof: previous_observation.metadata_proof(),
        metadata_log_epoch: ClusterEpoch::INITIAL,
        pending_metadata_command: previous_observation.pending_metadata_command(),
    }];
    let renewal = ControlPlaneCommand::RecordNodeHeartbeat {
        heartbeat: renewed_heartbeat,
        heartbeat_at_ms: renewal_at_ms,
        lease_deadline_ms: renewal_at_ms + 10_000,
        lease_horizon_authority: Some(horizon_authority),
    };
    let uncanonicalized = durable
        .apply_control_plane_command(renewal.clone())
        .unwrap()
        .into_snapshot();
    assert!(
        uncanonicalized
            .node(NodeId::new(2))
            .unwrap()
            .pg_observation(pg_id)
            .unwrap()
            .observed_at_ms()
            > previous_observation.observed_at_ms()
    );
    let volatile = durable
        .apply_covered_volatile_heartbeat(renewal)
        .unwrap()
        .expect("unchanged PG heartbeat renews only the volatile lease");
    assert_eq!(
        volatile
            .node(NodeId::new(2))
            .unwrap()
            .pg_observation(pg_id)
            .unwrap()
            .observed_at_ms(),
        previous_observation.observed_at_ms(),
    );
    assert!(
        volatile.node(NodeId::new(2)).unwrap().lease_deadline_ms()
            > durable.node(NodeId::new(2)).unwrap().lease_deadline_ms()
    );
    let promotion = volatile
        .promote_volatile_heartbeat_leases_command(&durable)
        .unwrap()
        .expect("renewed lease needs durable promotion");
    let promoted = durable
        .apply_control_plane_command(promotion)
        .unwrap()
        .into_snapshot();
    let volatile_begin_at_ms = begin_at_ms.max(renewal_at_ms + 1);
    let uncanonicalized_begin = uncanonicalized
        .begin_unavailable_pg_placement_transition_command(
            pg_id,
            NodeId::new(1),
            volatile_begin_at_ms,
        )
        .unwrap();
    assert!(promoted
        .apply_control_plane_command(uncanonicalized_begin)
        .unwrap_err()
        .to_string()
        .contains("begin authorization changed"));
    let live_begin = volatile
        .begin_unavailable_pg_placement_transition_command(
            pg_id,
            NodeId::new(1),
            volatile_begin_at_ms,
        )
        .unwrap();
    promoted
        .apply_control_plane_command(live_begin.clone())
        .expect("live begin authorization must apply after lease promotion");
    volatile
        .apply_control_plane_command(live_begin)
        .expect("same begin authorization must apply to the live overlay");
    let mut wrong_topology = stale_proposal.clone();
    let ControlPlaneCommand::BeginUnavailablePgPlacementTransitions { transitions, .. } =
        &mut wrong_topology
    else {
        unreachable!("builder returned the wrong command kind");
    };
    transitions[0].topology_digest[0] ^= 1;
    assert!(authority
        .snapshot()
        .apply_control_plane_command(wrong_topology)
        .is_err());
    let mut wrong_destination = stale_proposal.clone();
    let ControlPlaneCommand::BeginUnavailablePgPlacementTransitions { transitions, .. } =
        &mut wrong_destination
    else {
        unreachable!("builder returned the wrong command kind");
    };
    transitions[0].destination_acting_set.swap(0, 1);
    assert!(authority
        .snapshot()
        .apply_control_plane_command(wrong_destination)
        .is_err());
    let mut shortened_grace = stale_proposal.clone();
    let ControlPlaneCommand::BeginUnavailablePgPlacementTransitions { transitions, .. } =
        &mut shortened_grace
    else {
        unreachable!("builder returned the wrong command kind");
    };
    transitions[0].grace_cutoff_ms -= 1;
    assert!(authority
        .snapshot()
        .apply_control_plane_command(shortened_grace)
        .is_err());

    let before = authority.snapshot().clone();
    let early_scan = before.scan_unavailable_pg_reconciliation(
        UnavailablePgReconciliationCursor::start(),
        begin_at_ms - 1,
    );
    assert!(early_scan.candidate.is_none());
    assert_eq!(
        early_scan.next_cursor,
        UnavailablePgReconciliationCursor::start()
    );
    let ready_scan = before.scan_unavailable_pg_reconciliation(
        UnavailablePgReconciliationCursor::start(),
        begin_at_ms,
    );
    assert!(matches!(
        ready_scan.candidate,
        Some(UnavailablePgReconciliationCandidate::Begin {
            pg_id: candidate_pg_id,
            unavailable_node_id,
        }) if candidate_pg_id == pg_id && unavailable_node_id == NodeId::new(1)
    ));
    let too_early = before
        .begin_unavailable_pg_placement_transition_command(pg_id, NodeId::new(1), begin_at_ms - 1)
        .unwrap();
    assert!(before.apply_control_plane_command(too_early).is_err());
    assert_eq!(authority.snapshot(), &before);

    let mut reconciliation_cursor = UnavailablePgReconciliationCursor::start();
    let dispatched_work = authority
        .poll_unavailable_pg_reconciliation(&mut reconciliation_cursor, begin_at_ms)
        .unwrap()
        .expect("eligible unavailable PG must produce reconciliation work");
    assert_eq!(dispatched_work.pg_id(), pg_id);
    assert_eq!(
        dispatched_work.stage(),
        UnavailablePgReconciliationStage::MetadataTransfer
    );
    let transition = authority
        .snapshot()
        .unavailable_pg_placement_transition(pg_id)
        .unwrap()
        .clone();
    assert_eq!(
        transition.source_acting_set(),
        &[NodeId::new(1), NodeId::new(2), NodeId::new(3)]
    );
    assert_eq!(transition.source_node_id(), NodeId::new(2));
    assert_eq!(
        transition.destination_acting_set(),
        &[NodeId::new(4), NodeId::new(2), NodeId::new(3)]
    );
    assert_eq!(transition.unavailable_node(), &observation);
    assert_eq!(transition.destination_epoch(), None);
    let resume_scan = authority.snapshot().scan_unavailable_pg_reconciliation(
        UnavailablePgReconciliationCursor::start(),
        begin_at_ms + 1,
    );
    let Some(UnavailablePgReconciliationCandidate::Resume(resumed_work)) = resume_scan.candidate
    else {
        panic!("durable transition was not rediscovered after begin");
    };
    assert_eq!(resumed_work.pg_id(), pg_id);
    assert_eq!(
        resumed_work.transition_epoch(),
        transition.transition_epoch()
    );
    assert_eq!(
        resumed_work.destination_acting_set(),
        transition.destination_acting_set()
    );
    assert_eq!(
        resumed_work.stage(),
        UnavailablePgReconciliationStage::MetadataTransfer
    );
    assert_eq!(
        authority.snapshot().pg(pg_id).unwrap().state(),
        PgState::Peering
    );
    assert_eq!(
        authority.snapshot().pg(pg_id).unwrap().acting_set(),
        &[NodeId::new(2), NodeId::new(1), NodeId::new(3)]
    );
    let formatted = format_snapshot(authority.snapshot());
    assert!(formatted.contains(&format!(
        "unavailable_node=1,11,{},{},{}\n",
        hex_encode(b"/tmp/transition-node-1.sock"),
        observation.lease_deadline_ms(),
        observation.observed_at_ms()
    )));
    assert!(formatted.contains(&format!(
        "unavailable_pg_transition=7,{},-,{},{}",
        transition.transition_epoch().get(),
        3,
        hex_encode(&[0x5a; CONTROL_PLANE_TOPOLOGY_DIGEST_LEN])
    )));
    assert!(authority
        .snapshot()
        .apply_control_plane_command(ControlPlaneCommand::SetPgActingSet {
            pg_id,
            acting_set: vec![NodeId::new(2), NodeId::new(3), NodeId::new(4)],
        })
        .is_err());
    assert!(authority
        .snapshot()
        .apply_control_plane_command(ControlPlaneCommand::CompletePgPeering {
            pg_id,
            primary: NodeId::new(2),
            node_incarnation: 12,
            complete_at_ms: begin_at_ms + 1,
        })
        .is_err());

    let replayed = authority
        .snapshot()
        .apply_control_plane_command(stale_proposal.clone())
        .unwrap();
    assert!(!replayed.changed());
    assert_eq!(replayed.snapshot(), authority.snapshot());
    assert_persisted_snapshot_matches_authority(&authority, &store);
    let restarted = reopen_file_authority(&store);
    assert_eq!(
        restarted
            .snapshot()
            .unavailable_pg_placement_transition(pg_id),
        Some(&transition)
    );
    drop(authority);
    let mut authority = restarted;

    let before_generic_install = authority.snapshot().clone();
    assert!(authority
        .set_pg_acting_set_with_metadata_transfer(
            pg_id,
            vec![NodeId::new(4), NodeId::new(2), NodeId::new(3)],
            PgMetadataTransferProof::new(
                dispatched_work.mutation_binding().source_epoch(),
                active_proof,
            ),
        )
        .is_err());
    assert_eq!(authority.snapshot(), &before_generic_install);
    let install = authorize_and_publish_unavailable_transition(
        &mut authority,
        dispatched_work.mutation_binding(),
        active_proof,
        0x71,
    );
    authority
        .install_unavailable_pg_placement_transitions_batch(
            std::slice::from_ref(&install),
            install.expected_destination_epoch,
        )
        .unwrap();
    let destination_epoch = authority.snapshot().cluster_epoch();
    assert_eq!(
        authority
            .snapshot()
            .unavailable_pg_placement_transition(pg_id)
            .unwrap()
            .destination_epoch(),
        Some(destination_epoch)
    );
    let replayed_after_transfer = authority
        .snapshot()
        .apply_control_plane_command(stale_proposal.clone())
        .unwrap();
    assert!(!replayed_after_transfer.changed());
    assert_eq!(replayed_after_transfer.snapshot(), authority.snapshot());
    let pg = authority.snapshot().pg(pg_id).unwrap();
    assert_eq!(
        pg.peering_metadata_transfer_source_node_id(),
        Some(NodeId::new(2))
    );
    let complete_at_ms = failed_deadline + CONTROL_PLANE_CLOCK_SKEW_BUDGET_MS + 1;
    for node_id in [4, 2, 3] {
        heartbeat_with_pg_proof_and_lease_duration(
            &mut authority,
            node_id,
            pg_id.get(),
            PgState::Peering,
            active_proof,
            false,
            (complete_at_ms - 10 + u64::from(node_id), 10_000),
        );
    }
    let primary_incarnation = node_incarnation(&authority, 4);
    assert!(matches!(
        authority.snapshot().apply_control_plane_command(
            ControlPlaneCommand::CompletePgPeering {
                pg_id,
                primary: NodeId::new(4),
                node_incarnation: primary_incarnation,
                complete_at_ms,
            }
        ),
        Err(ControlPlaneError::CommandDecode { message })
            if message.contains("not payload-write ready")
    ));
    assert!(matches!(
        authority.snapshot().apply_control_plane_command(
            ControlPlaneCommand::CompleteReadyPgPeerings {
                ready_at_ms: complete_at_ms,
                ready: vec![ReadyPgPeeringCompletion {
                    pg_id,
                    primary: NodeId::new(4),
                    node_incarnation: primary_incarnation,
                    active_metadata_proof: active_proof,
                    active_metadata_proof_epoch: authority.snapshot().cluster_epoch(),
                }],
            }
        ),
        Err(ControlPlaneError::CommandDecode { message })
            if message.contains("not payload-write ready")
    ));
    let completion_command = authority
        .snapshot()
        .complete_unavailable_pg_placement_transition_command(&dispatched_work, complete_at_ms)
        .unwrap();
    let ControlPlaneCommand::CompleteUnavailablePgPlacementTransitions {
        ready_at_ms: completion_ready_at_ms,
        transitions,
    } = &completion_command
    else {
        unreachable!("unavailable placement completion builder returned the wrong command kind");
    };
    let mut forged_standalone_readiness = authority.snapshot().clone();
    forged_standalone_readiness
        .unavailable_pg_placement_transitions
        .get_mut(&pg_id)
        .unwrap()
        .payload_readiness = Some(UnavailablePgPayloadReadiness {
        pg_id,
        transition_epoch: transitions[0].transition_epoch,
        destination_epoch: transitions[0].destination_epoch,
        topology_generation: transitions[0].topology_generation,
        topology_digest: transitions[0].topology_digest,
        ready_at_ms: *completion_ready_at_ms,
        destinations: transitions[0].destinations.clone(),
    });
    assert!(forged_standalone_readiness
        .validate_current_state_invariants()
        .unwrap_err()
        .contains("standalone payload readiness"));
    let destination_lease_deadline = authority
        .snapshot()
        .node(NodeId::new(4))
        .unwrap()
        .lease_deadline_ms()
        .unwrap();
    let expired_destination = authority
        .snapshot()
        .apply_control_plane_command(ControlPlaneCommand::ExpireHeartbeatLeases {
            expire_at_ms: destination_lease_deadline,
        })
        .unwrap()
        .into_snapshot();
    assert!(expired_destination
        .apply_control_plane_command(completion_command.clone())
        .is_err());
    assert_eq!(
        expired_destination.pg(pg_id).unwrap().state(),
        PgState::Peering
    );

    let mut changed_incarnation = authority.snapshot().clone();
    changed_incarnation
        .nodes
        .get_mut(&NodeId::new(2))
        .unwrap()
        .node_incarnation += 1;
    assert!(changed_incarnation
        .apply_control_plane_command(completion_command.clone())
        .unwrap_err()
        .to_string()
        .contains("payload"));

    let renewal_at_ms = complete_at_ms + 1;
    let renewed_lease = heartbeat_with_pg_proof_and_lease_duration(
        &mut authority,
        4,
        pg_id.get(),
        PgState::Peering,
        active_proof,
        false,
        (renewal_at_ms, 10_000),
    );
    let renewed_node = authority.snapshot().node(NodeId::new(4)).unwrap();
    assert_eq!(
        renewed_node.node_incarnation(),
        node_incarnation(&authority, 4)
    );
    assert_ne!(
        renewed_lease.lease_deadline_ms(),
        destination_lease_deadline
    );
    assert!(authority
        .snapshot()
        .apply_control_plane_command(completion_command)
        .is_err());
    assert_eq!(
        authority.snapshot().pg(pg_id).unwrap().state(),
        PgState::Peering
    );
    assert!(authority
        .snapshot()
        .ready_pg_peering_completions(renewal_at_ms)
        .unwrap()
        .is_empty());

    let republished_ready_at_ms = renewal_at_ms + 1;
    let readiness_work = authority
        .poll_unavailable_pg_reconciliation(
            &mut UnavailablePgReconciliationCursor::start(),
            republished_ready_at_ms,
        )
        .unwrap();
    let readiness_work = readiness_work.expect("stale readiness must be rediscovered");
    assert_eq!(
        readiness_work.stage(),
        UnavailablePgReconciliationStage::PayloadReadiness
    );
    let replayable_completion = authority
        .snapshot()
        .complete_unavailable_pg_placement_transition_command(
            &readiness_work,
            republished_ready_at_ms,
        )
        .unwrap();
    let (completion_request, completion_ready_at_ms) =
        completion_request_from_command(replayable_completion.clone());
    let projected = authority
        .snapshot()
        .apply_control_plane_command(replayable_completion)
        .unwrap()
        .into_snapshot();
    let mut after_later_commit = projected.clone();
    after_later_commit.max_committed_timestamp_ms = Some(completion_ready_at_ms + 1_000);
    let later_epoch = after_later_commit.cluster_epoch;
    let current_pg = after_later_commit.pgs.get_mut(&pg_id).unwrap();
    current_pg.active_primary = Some(NodeId::new(2));
    current_pg.active_metadata_proof = Some(PgMetadataProof::empty());
    current_pg.active_metadata_proof_epoch = Some(later_epoch);
    let replay = after_later_commit
        .validate_unavailable_pg_transition_completion_batch(
            vec![completion_request.clone()],
            completion_ready_at_ms,
        )
        .unwrap();
    assert!(after_later_commit
        .apply_validated_unavailable_pg_transition_completions(replay, completion_ready_at_ms,)
        .unwrap()
        .is_none());

    let mut altered = completion_request.clone();
    altered.destination_epoch = next_epoch(altered.destination_epoch).unwrap();
    assert!(after_later_commit
        .validate_unavailable_pg_transition_completion_batch(vec![altered], completion_ready_at_ms,)
        .is_err());
    let mut altered = completion_request.clone();
    altered.topology_generation += 1;
    assert!(after_later_commit
        .validate_unavailable_pg_transition_completion_batch(vec![altered], completion_ready_at_ms,)
        .is_err());
    let mut altered = completion_request.clone();
    altered.topology_digest[0] ^= 1;
    assert!(after_later_commit
        .validate_unavailable_pg_transition_completion_batch(vec![altered], completion_ready_at_ms,)
        .is_err());
    let mut altered = completion_request.clone();
    altered.destinations[0].lease_deadline_ms += 1;
    assert!(after_later_commit
        .validate_unavailable_pg_transition_completion_batch(vec![altered], completion_ready_at_ms,)
        .is_err());
    let mut altered = completion_request.clone();
    altered.completion.primary = NodeId::new(2);
    assert!(after_later_commit
        .validate_unavailable_pg_transition_completion_batch(vec![altered], completion_ready_at_ms,)
        .is_err());
    let mut altered = completion_request.clone();
    altered.completion.node_incarnation += 1;
    assert!(after_later_commit
        .validate_unavailable_pg_transition_completion_batch(vec![altered], completion_ready_at_ms,)
        .is_err());
    let mut altered = completion_request.clone();
    altered.completion.active_metadata_proof = PgMetadataProof::empty();
    assert!(after_later_commit
        .validate_unavailable_pg_transition_completion_batch(vec![altered], completion_ready_at_ms,)
        .is_err());
    let mut altered = completion_request.clone();
    altered.completion.active_metadata_proof_epoch =
        next_epoch(altered.completion.active_metadata_proof_epoch).unwrap();
    assert!(after_later_commit
        .validate_unavailable_pg_transition_completion_batch(vec![altered], completion_ready_at_ms,)
        .is_err());
    assert!(after_later_commit
        .validate_unavailable_pg_transition_completion_batch(
            vec![completion_request.clone()],
            completion_ready_at_ms + 1,
        )
        .is_err());
    assert!(after_later_commit
        .validate_unavailable_pg_transition_completion_batch(
            vec![completion_request.clone(), completion_request],
            completion_ready_at_ms,
        )
        .unwrap_err()
        .to_string()
        .contains("strictly increasing"));

    assert!(authority
        .complete_unavailable_pg_reconciliation(&readiness_work, republished_ready_at_ms)
        .unwrap());
    assert_eq!(
        authority.snapshot().pg(pg_id).unwrap().state(),
        PgState::Active
    );
    let active_after_completion = authority.snapshot().clone();
    assert!(authority
        .fence_unavailable_pg_transition_with_source_lease(
            dispatched_work.mutation_binding().clone(),
        )
        .is_err());
    assert_eq!(authority.snapshot(), &active_after_completion);
    assert!(authority
        .set_pg_acting_set_with_metadata_transfer(
            pg_id,
            dispatched_work.destination_acting_set().to_vec(),
            PgMetadataTransferProof::new(
                dispatched_work.mutation_binding().source_epoch(),
                active_proof,
            ),
        )
        .is_err());
    assert_eq!(authority.snapshot(), &active_after_completion);
    let terminal_transition = authority
        .snapshot()
        .retained_unavailable_pg_placement_transitions()
        .find(|transition| transition.pg_id() == pg_id)
        .expect("historical payload still requires the durable transition");
    assert_eq!(
        terminal_transition.destination_epoch(),
        Some(destination_epoch)
    );
    let replayed_after_activation = authority
        .snapshot()
        .apply_control_plane_command(stale_proposal.clone())
        .unwrap();
    assert!(!replayed_after_activation.changed());
    assert_eq!(replayed_after_activation.snapshot(), authority.snapshot());
    for node_id in [4, 2, 3] {
        heartbeat_with_pg_proof_and_lease_duration(
            &mut authority,
            node_id,
            pg_id.get(),
            PgState::Active,
            active_proof,
            false,
            (complete_at_ms + 10 + u64::from(node_id), 10_000),
        );
    }
    authority
        .set_pg_acting_set(pg_id, vec![NodeId::new(4), NodeId::new(3), NodeId::new(2)])
        .unwrap();
    let after_later_route = authority.snapshot().clone();
    let replayed_after_later_route = after_later_route
        .apply_control_plane_command(stale_proposal)
        .unwrap();
    assert!(!replayed_after_later_route.changed());
    assert_eq!(replayed_after_later_route.snapshot(), &after_later_route);
    assert_persisted_snapshot_matches_authority(&authority, &store);

    let transition_key = (pg_id, transition.transition_epoch());
    let mut forged_singleton_digest = after_later_route.clone();
    forged_singleton_digest
        .retained_unavailable_pg_placement_transitions
        .get_mut(&transition_key)
        .unwrap()
        .completion_batch_receipt
        .as_mut()
        .unwrap()
        .identity
        .members_digest[0] ^= 0x40;
    let error = parse_snapshot(&format_snapshot(&forged_singleton_digest)).unwrap_err();
    assert!(
        error.to_string().contains("durable member evidence"),
        "unexpected singleton completion digest error: {error}"
    );

    let mut forged_gapped_epochs = after_later_route.clone();
    let gapped_receipt = forged_gapped_epochs
        .retained_unavailable_pg_placement_transitions
        .get_mut(&transition_key)
        .unwrap()
        .completion_batch_receipt
        .as_mut()
        .unwrap();
    gapped_receipt.source_epoch = transition.source_epoch();
    assert_ne!(
        next_epoch(gapped_receipt.source_epoch).unwrap(),
        gapped_receipt.target_epoch
    );
    let error = parse_snapshot(&format_snapshot(&forged_gapped_epochs)).unwrap_err();
    assert!(
        error.to_string().contains("completion activation epochs"),
        "unexpected gapped completion epoch error: {error}"
    );

    let mut forged_future_epochs = after_later_route.clone();
    let future_receipt = forged_future_epochs
        .retained_unavailable_pg_placement_transitions
        .get_mut(&transition_key)
        .unwrap()
        .completion_batch_receipt
        .as_mut()
        .unwrap();
    future_receipt.source_epoch = after_later_route.cluster_epoch();
    future_receipt.target_epoch = next_epoch(future_receipt.source_epoch).unwrap();
    let error = parse_snapshot(&format_snapshot(&forged_future_epochs)).unwrap_err();
    assert!(
        error.to_string().contains("completion activation epochs"),
        "unexpected future completion epoch error: {error}"
    );

    let mut forged_grace = authority.snapshot().clone();
    forged_grace
        .retained_unavailable_pg_placement_transitions
        .get_mut(&(pg_id, transition.transition_epoch()))
        .unwrap()
        .grace_cutoff_ms -= 1;
    let forged_grace_error = forged_grace
        .validate_current_state_invariants()
        .unwrap_err();
    assert!(
        forged_grace_error.contains("lease or grace evidence"),
        "unexpected forged grace error: {forged_grace_error}"
    );

    let mut coordinated_time_forgery = authority.snapshot().clone();
    let coordinated = coordinated_time_forgery
        .retained_unavailable_pg_placement_transitions
        .get_mut(&(pg_id, transition.transition_epoch()))
        .unwrap();
    coordinated.unavailable_node.lease_deadline_ms -= 1;
    coordinated.unavailable_node.observed_at_ms -= 1;
    coordinated.grace_cutoff_ms -= 1;
    let coordinated_time_error = coordinated_time_forgery
        .validate_current_state_invariants()
        .unwrap_err();
    assert!(
        coordinated_time_error.contains("invalid begin authorization"),
        "unexpected coordinated time forgery error: {coordinated_time_error}"
    );

    let mut invalid_provenance_forgery = authority.snapshot().clone();
    let invalid_authorization = &mut invalid_provenance_forgery
        .retained_unavailable_pg_placement_transitions
        .get_mut(&(pg_id, transition.transition_epoch()))
        .unwrap()
        .begin_authorization;
    invalid_authorization.source_metadata_floor_epoch = None;
    invalid_authorization.source_metadata_floor_imported = true;
    invalid_authorization
        .source_route
        .peering_metadata_proof_floor_epoch = None;
    invalid_authorization
        .source_route
        .peering_metadata_proof_floor_imported = true;
    let invalid_historical_route = invalid_provenance_forgery
        .history
        .iter_mut()
        .find(|record| record.cluster_epoch() == transition.source_epoch())
        .and_then(|record| record.pgs.iter_mut().find(|record| record.pg_id == pg_id))
        .expect("transition source route must be retained in history");
    invalid_historical_route.peering_metadata_proof_floor_epoch = None;
    invalid_historical_route.peering_metadata_proof_floor_imported = true;
    let invalid_provenance_error = invalid_provenance_forgery
        .validate_current_state_invariants()
        .unwrap_err();
    assert!(
        invalid_provenance_error.contains("imported floor provenance without a floor epoch"),
        "unexpected proof-provenance forgery error: {invalid_provenance_error}"
    );
    let invalid_provenance_parse_error =
        parse_snapshot(&format_snapshot(&invalid_provenance_forgery)).unwrap_err();
    assert!(
        invalid_provenance_parse_error
            .to_string()
            .contains("imported floor provenance without a floor epoch"),
        "unexpected parsed proof-provenance error: {invalid_provenance_parse_error}"
    );

    let mut coordinated_proof_forgery = authority.snapshot().clone();
    let forged_proof = PgMetadataProof::current(18, 0x1818, 0x2828);
    let forged_transition = coordinated_proof_forgery
        .retained_unavailable_pg_placement_transitions
        .get_mut(&(pg_id, transition.transition_epoch()))
        .unwrap();
    let forged_authorization = &mut forged_transition.begin_authorization;
    forged_authorization.source_metadata_floor = forged_proof;
    forged_authorization.source_metadata_proof = forged_proof;
    forged_authorization
        .source_route
        .peering_metadata_proof_floor = Some(forged_proof);
    let destination_transfer = forged_transition
        .destination_route
        .as_ref()
        .unwrap()
        .peering_metadata_transfer
        .unwrap();
    forged_transition
        .destination_route
        .as_mut()
        .unwrap()
        .peering_metadata_transfer =
        Some(PgMetadataTransferProof::new_with_imported_metadata_proof(
            destination_transfer.source_epoch(),
            forged_proof,
            destination_transfer.metadata_proof(),
        ));
    let coordinated_proof_error = coordinated_proof_forgery
        .validate_current_state_invariants()
        .unwrap_err();
    assert!(
        coordinated_proof_error.contains("invalid destination install evidence"),
        "unexpected coordinated proof error: {coordinated_proof_error}"
    );
    let coordinated_proof_parse_error =
        parse_snapshot(&format_snapshot(&coordinated_proof_forgery)).unwrap_err();
    assert!(
        coordinated_proof_parse_error
            .to_string()
            .contains("source route does not match retained CAS evidence"),
        "unexpected parsed coordinated-proof error: {coordinated_proof_parse_error}"
    );

    let mut forged_source = authority.snapshot().clone();
    forged_source
        .retained_unavailable_pg_placement_transitions
        .get_mut(&(pg_id, transition.transition_epoch()))
        .unwrap()
        .source_node_id = NodeId::new(3);
    assert!(forged_source
        .validate_current_state_invariants()
        .unwrap_err()
        .contains("invalid begin authorization"));

    let mut forged_epoch_order = authority.snapshot().clone();
    forged_epoch_order
        .retained_unavailable_pg_placement_transitions
        .get_mut(&(pg_id, transition.transition_epoch()))
        .unwrap()
        .destination_epoch = Some(transition.transition_epoch());
    assert!(forged_epoch_order
        .validate_current_state_invariants()
        .unwrap_err()
        .contains("invalid epoch ordering"));

    let mut forged_substitution = authority.snapshot().clone();
    forged_substitution
        .retained_unavailable_pg_placement_transitions
        .get_mut(&(pg_id, transition.transition_epoch()))
        .unwrap()
        .destination_acting_set
        .swap(0, 1);
    let forged_substitution_error = forged_substitution
        .validate_current_state_invariants()
        .unwrap_err();
    assert!(
        forged_substitution_error.contains("staging evidence does not match its authorization"),
        "unexpected forged-substitution error: {forged_substitution_error}"
    );

    let mut forged_history = authority.snapshot().clone();
    let historical_route = forged_history
        .history
        .iter_mut()
        .filter(|record| record.cluster_epoch() >= transition.transition_epoch())
        .find_map(|record| record.pgs.iter_mut().find(|record| record.pg_id == pg_id))
        .expect("transition route must be retained in history");
    historical_route.acting_set.swap(0, 1);
    assert!(forged_history
        .validate_publication_invariants()
        .unwrap_err()
        .contains("does not match retained cluster-map history"));

    let mut forged_destination_provenance = authority.snapshot().clone();
    let forged_transition = forged_destination_provenance
        .retained_unavailable_pg_placement_transitions
        .get_mut(&(pg_id, transition.transition_epoch()))
        .unwrap();
    forged_transition
        .destination_route
        .as_mut()
        .unwrap()
        .peering_metadata_transfer = None;
    assert!(forged_destination_provenance
        .validate_current_state_invariants()
        .unwrap_err()
        .contains("invalid destination-route evidence"));

    let protected_epochs = [
        transition.source_epoch(),
        transition.transition_epoch(),
        destination_epoch,
    ];
    let mut advanced = authority.snapshot().clone();
    for step in 0..(CLUSTER_MAP_HISTORY_LIMIT + 16) {
        let membership = if step % 2 == 0 {
            NodeMembershipState::Active
        } else {
            NodeMembershipState::Draining
        };
        advanced = advanced
            .apply_control_plane_command(ControlPlaneCommand::SetNodeMembership {
                node_id: NodeId::new(99),
                membership,
            })
            .unwrap()
            .into_snapshot();
    }
    for epoch in protected_epochs {
        assert!(
            historical_pg_exists_at_epoch(&advanced.history, &advanced.pgs, pg_id, epoch),
            "terminal transition lost protected PG route at epoch {epoch}"
        );
    }
    let encoded = format_snapshot(&advanced);
    assert_eq!(parse_snapshot(&encoded).unwrap(), advanced);
}

#[test]
fn published_pending_command_blocks_unavailable_pg_transfer_source() {
    let (_tmp, _store, mut authority, pg_id) = certified_spare_authority();
    for node_id in 1..=4 {
        heartbeat_spare_node(&mut authority, node_id, 1_000 + u64::from(node_id));
    }
    let floor = PgMetadataProof::current(17, 0x1717, 0x2727);
    let published = PgMetadataProof::current(18, 0x1818, 0x2828);
    for node_id in 1..=3 {
        heartbeat_with_pg_proof_and_lease_duration(
            &mut authority,
            node_id,
            pg_id.get(),
            PgState::Peering,
            floor,
            false,
            (2_000 + u64::from(node_id), 10_000),
        );
    }
    authority
        .complete_pg_peering(
            pg_id,
            NodeId::new(1),
            node_incarnation(&authority, 1),
            2_004,
        )
        .unwrap();
    let pending_epoch = authority.snapshot().cluster_epoch();
    let pending =
        PendingMetadataCommandObservation::new(pending_epoch, NonZeroU64::new(18).unwrap(), 0xfeed);
    for node_id in 1..=3 {
        heartbeat_with_pg_proof_and_lease_duration(
            &mut authority,
            node_id,
            pg_id.get(),
            PgState::Active,
            floor,
            false,
            (
                3_000 + u64::from(node_id),
                if node_id == 3 { 100 } else { 10_000 },
            ),
        );
    }
    // Publication reaches both survivors, but the primary cannot remove its
    // slot while the failed trailing replica remains in the historical route.
    let mut primary = heartbeat_from_record(&authority, 1, pending_epoch, 3_101);
    primary.pg_observations = vec![NodePgHeartbeatObservation {
        pg_id,
        state: PgState::Active,
        metadata_proof: published,
        metadata_log_epoch: ClusterEpoch::INITIAL,
        pending_metadata_command: Some(pending),
    }];
    primary.requested_lease_duration_ms = 10_000;
    authority.heartbeat(primary, 3_101).unwrap();
    heartbeat_with_pg_proof_and_lease_duration(
        &mut authority,
        2,
        pg_id.get(),
        PgState::Active,
        published,
        false,
        (3_102, 10_000),
    );
    assert_eq!(
        authority
            .snapshot()
            .pg(pg_id)
            .unwrap()
            .active_metadata_proof(),
        Some(floor)
    );
    let failed_deadline = authority
        .snapshot()
        .node(NodeId::new(3))
        .unwrap()
        .lease_deadline_ms()
        .unwrap();
    authority.expire_heartbeat_leases(failed_deadline).unwrap();
    for node_id in [1, 2, 4] {
        heartbeat_spare_node(
            &mut authority,
            node_id,
            failed_deadline + u64::from(node_id),
        );
    }
    let peering_at_ms = authority.snapshot().max_committed_timestamp_ms().unwrap() + 1;
    for node_id in 1..=2 {
        let now_ms = peering_at_ms + u64::from(node_id);
        let mut request = heartbeat_from_record(
            &authority,
            node_id,
            authority.snapshot().cluster_epoch(),
            now_ms,
        );
        request.pg_observations = vec![NodePgHeartbeatObservation {
            pg_id,
            state: PgState::Peering,
            metadata_proof: published,
            metadata_log_epoch: ClusterEpoch::INITIAL,
            pending_metadata_command: (node_id == 1).then_some(pending),
        }];
        request.requested_lease_duration_ms = 10_000;
        authority.heartbeat(request, now_ms).unwrap();
    }
    let listing = authority.snapshot().pending_metadata_command_recoveries();
    assert!(listing.tasks().iter().any(|task| task.pg_id() == pg_id));
    let snapshot = authority.snapshot().clone();
    let begin_at_ms = snapshot
        .unavailable_node_observation(NodeId::new(3))
        .unwrap()
        .observed_at_ms()
        + 50;
    assert!(snapshot
        .begin_unavailable_pg_placement_transition_command(pg_id, NodeId::new(3), begin_at_ms)
        .unwrap_err()
        .to_string()
        .contains("no proof-qualified serving metadata-transfer source"));
    assert_eq!(authority.snapshot(), &snapshot);
    assert!(pending_epoch < snapshot.cluster_epoch());
}

#[test]
fn unavailable_pg_transition_successor_consumes_retained_lineage_tip() {
    let (_tmp, _store, mut authority, pg_id) = certified_spare_authority_with_policy(
        5,
        test_certified_storage_placement_policy((1..=5).map(NodeId::new), 3, 10),
    );
    for node_id in 1..=5 {
        heartbeat_spare_node(&mut authority, node_id, 1_000 + u64::from(node_id));
    }
    let proof = PgMetadataProof::current(17, 0x1717, 0x2727);
    for node_id in 1..=3 {
        heartbeat_with_pg_proof_and_lease_duration(
            &mut authority,
            node_id,
            pg_id.get(),
            PgState::Peering,
            proof,
            false,
            (2_000 + u64::from(node_id), 10_000),
        );
    }
    authority
        .complete_pg_peering(
            pg_id,
            NodeId::new(1),
            node_incarnation(&authority, 1),
            2_004,
        )
        .unwrap();
    for node_id in 1..=3 {
        heartbeat_with_pg_proof_and_lease_duration(
            &mut authority,
            node_id,
            pg_id.get(),
            PgState::Active,
            proof,
            false,
            (
                3_000 + u64::from(node_id),
                if node_id == 1 { 100 } else { 10_000 },
            ),
        );
    }
    let first_deadline = authority
        .snapshot()
        .node(NodeId::new(1))
        .unwrap()
        .lease_deadline_ms()
        .unwrap();
    authority.expire_heartbeat_leases(first_deadline).unwrap();
    for round in 0..2 {
        for node_id in 2..=5 {
            heartbeat_spare_node(
                &mut authority,
                node_id,
                first_deadline + round * 10 + u64::from(node_id),
            );
        }
    }
    for node_id in [2, 3] {
        heartbeat_with_pg_proof_and_lease_duration(
            &mut authority,
            node_id,
            pg_id.get(),
            PgState::Peering,
            proof,
            false,
            (first_deadline + 30 + u64::from(node_id), 10_000),
        );
    }
    let first_begin_at = (authority
        .snapshot()
        .unavailable_node_observation(NodeId::new(1))
        .unwrap()
        .observed_at_ms()
        + 10)
        .max(authority.snapshot().max_committed_timestamp_ms().unwrap());
    for node_id in 2..=5 {
        let node = authority.snapshot().node(NodeId::new(node_id)).unwrap();
        assert!(
            node.can_serve_primary(authority.snapshot().cluster_epoch(), first_begin_at),
            "node {node_id} should serve at {first_begin_at}: {node:?}"
        );
    }
    authority
        .begin_unavailable_pg_placement_transition(pg_id, NodeId::new(1), first_begin_at)
        .unwrap();
    let first_work = authority
        .poll_unavailable_pg_reconciliation(
            &mut UnavailablePgReconciliationCursor::start(),
            first_begin_at,
        )
        .unwrap()
        .expect("first transition remains recoverable");
    let first_transition_epoch = authority
        .snapshot()
        .unavailable_pg_placement_transition(pg_id)
        .unwrap()
        .transition_epoch();
    let first_transition = authority
        .snapshot()
        .unavailable_pg_placement_transition(pg_id)
        .unwrap();
    let first_binding = UnavailablePgTransitionMutationBinding::new(
        first_transition.pg_id,
        first_transition.transition_epoch,
        first_transition.source_epoch,
        first_transition.source_acting_set.clone(),
        first_transition.destination_acting_set.clone(),
    );
    let first_artifact_target_epoch = next_epoch(authority.snapshot().cluster_epoch()).unwrap();
    let first_artifact =
        crate::pg_store::canonical_nonempty_staged_metadata_transfer_artifact_for_test(
            &first_binding,
            first_artifact_target_epoch,
        );
    let first_staging_request = UnavailablePgStagingIntentAuthorizationRequest {
        unavailable_transition: first_binding,
        staging_generation: first_transition.transition_epoch.get(),
        artifact_target_epoch: first_artifact_target_epoch,
        artifact_digest: checksum::sha256::digest(&first_artifact),
        artifact_length: u64::try_from(first_artifact.len()).unwrap(),
        artifact_format_version: crate::pg_store::METADATA_TRANSFER_STAGED_ARTIFACT_FORMAT_VERSION,
    };
    ControlPlaneLinearizedCommandSink::submit_control_plane_command(
        &mut authority,
        ControlPlaneCommand::AuthorizeUnavailablePgStagingIntents {
            authorizations: vec![first_staging_request.clone()],
        },
    )
    .unwrap();

    let superseded_tmp = test_util::tempdir();
    let superseded_store =
        FileControlPlaneStore::new(superseded_tmp.path().join("superseded-control-plane.state"));
    superseded_store
        .checkpoint(None, authority.snapshot())
        .unwrap();
    let mut superseded = SingleAuthorityControlPlane::open(superseded_store).unwrap();
    let first_replacement_deadline = superseded
        .snapshot()
        .node(NodeId::new(4))
        .unwrap()
        .lease_deadline_ms()
        .unwrap();
    superseded
        .expire_heartbeat_leases(first_replacement_deadline)
        .unwrap();
    for round in 0..2 {
        for node_id in [2, 3, 5] {
            heartbeat_spare_node(
                &mut superseded,
                node_id,
                first_replacement_deadline + round * 10 + u64::from(node_id),
            );
        }
    }
    let superseded_proof_at = superseded.snapshot().max_committed_timestamp_ms().unwrap() + 1;
    for node_id in [2, 3] {
        heartbeat_with_pg_proof_and_lease_duration(
            &mut superseded,
            node_id,
            pg_id.get(),
            PgState::Peering,
            proof,
            false,
            (superseded_proof_at + u64::from(node_id), 10_000),
        );
    }
    let superseding_begin_at = (superseded
        .snapshot()
        .unavailable_node_observation(NodeId::new(1))
        .unwrap()
        .observed_at_ms()
        + 10)
        .max(superseded.snapshot().max_committed_timestamp_ms().unwrap());
    superseded
        .begin_unavailable_pg_placement_transition(pg_id, NodeId::new(1), superseding_begin_at)
        .unwrap();
    let superseding_transition_epoch = superseded
        .snapshot()
        .unavailable_pg_placement_transition(pg_id)
        .unwrap()
        .transition_epoch();
    let retained_uninstalled = superseded
        .snapshot()
        .retained_unavailable_pg_placement_transitions()
        .find(|transition| transition.transition_epoch() == first_transition_epoch)
        .unwrap();
    assert_eq!(retained_uninstalled.destination_epoch(), None);
    assert!(retained_uninstalled.staging_authorization.is_some());
    let retained_destination_acting_set = retained_uninstalled.destination_acting_set.clone();
    superseded
        .snapshot()
        .validate_current_state_invariants()
        .unwrap();
    assert_eq!(
        parse_snapshot(&format_snapshot(superseded.snapshot())).unwrap(),
        *superseded.snapshot()
    );
    let superseded_replay = superseded
        .snapshot()
        .apply_control_plane_command(ControlPlaneCommand::AuthorizeUnavailablePgStagingIntents {
            authorizations: vec![first_staging_request.clone()],
        })
        .unwrap();
    assert!(!superseded_replay.changed());
    assert_eq!(superseded_replay.snapshot(), superseded.snapshot());
    let first_intent = crate::pg_store::MetadataTransferStagingIntent::for_unavailable_transition(
        &first_staging_request.unavailable_transition,
        first_staging_request.artifact_digest,
        first_staging_request.artifact_length,
        first_staging_request.artifact_format_version,
    )
    .unwrap();
    let mut destination_stores = Vec::new();
    let mut superseded_tombstones = Vec::new();
    for (index, node_id) in retained_destination_acting_set.iter().copied().enumerate() {
        let node = superseded.snapshot().node(node_id).unwrap();
        let actor = crate::pg_store::MetadataTransferStagingNodeIdentity::new(
            node_id,
            node.node_incarnation(),
            node.endpoint().to_owned(),
        )
        .unwrap();
        let authorization = superseded
            .snapshot()
            .committed_unavailable_pg_staging_authorization(&first_staging_request, node_id)
            .unwrap();
        let temp = test_util::tempdir();
        let store = crate::pg_store::MetadataTransferStagingStore::open(
            temp.path(),
            actor,
            crate::pg_store::MetadataTransferStagingLimits::new(8, 1 << 20, 8 << 20).unwrap(),
        )
        .unwrap();
        if index == 0 {
            store
                .create_intent_authorized(&authorization, &first_intent)
                .unwrap();
            store
                .publish_artifact_authorized(&authorization, &first_intent, &first_artifact)
                .unwrap();
        }
        let tombstone = store
            .tombstone_authorized(&authorization, &first_intent)
            .unwrap();
        let evidence = crate::pg_store::decode_staging_evidence(tombstone.as_bytes()).unwrap();
        let page = store.next_evidence_page().unwrap().unwrap();
        let apply_receipt = superseded
            .apply_metadata_transfer_staging_evidence_page(
                page.operation_payload().to_vec(),
                page.page_digest(),
            )
            .unwrap();
        store
            .record_evidence_apply_receipt(
                &page,
                &crate::pg_store::decode_staging_evidence_apply_receipt(&apply_receipt).unwrap(),
            )
            .unwrap();
        superseded_tombstones.push(MetadataTransferStagingTombstoneBinding {
            node_id,
            node_incarnation: evidence.actor().node_incarnation(),
            endpoint: evidence.actor().endpoint().to_owned(),
            evidence_digest: checksum::sha256::digest(tombstone.as_bytes()),
        });
        destination_stores.push((temp, store, authorization));
    }
    superseded_tombstones.sort_by_key(|tombstone| tombstone.node_id);
    let cancellation = FinalizeMetadataTransferStagingGenerationRequest {
        unavailable_transition: first_staging_request.unavailable_transition.clone(),
        staging_generation: first_staging_request.staging_generation,
        disposition: MetadataTransferStagingCleanupDisposition::Superseded {
            successor_transition_epoch: superseding_transition_epoch,
        },
        tombstones: superseded_tombstones,
    };
    let cancelled = superseded
        .finalize_metadata_transfer_staging_generation(cancellation.clone())
        .unwrap();
    assert_eq!(
        cancelled.metadata_transfer_staging_finalized_floors
            [&(pg_id, first_staging_request.staging_generation)]
            .disposition,
        cancellation.disposition
    );
    assert_eq!(
        superseded
            .finalize_metadata_transfer_staging_generation(cancellation)
            .unwrap(),
        cancelled
    );
    let mut installed_cancellation_forgery = cancelled.clone();
    installed_cancellation_forgery
        .retained_unavailable_pg_placement_transitions
        .get_mut(&(pg_id, first_transition_epoch))
        .unwrap()
        .destination_epoch = Some(superseding_transition_epoch);
    let error = installed_cancellation_forgery
        .validate_current_state_invariants()
        .unwrap_err();
    assert!(
        error.contains("not an exact pre-install successor"),
        "unexpected installed cancellation invariant error: {error}"
    );
    for (_, store, authorization) in &destination_stores {
        assert!(matches!(
            store.create_intent_authorized(authorization, &first_intent),
            Err(crate::pg_store::MetadataTransferStagingError::GenerationRetired)
        ));
    }
    let mut superseded_forgery = superseded.snapshot().clone();
    forge_staging_authorization_at_epoch(
        &mut superseded_forgery,
        &[(pg_id, first_transition_epoch)],
        superseding_transition_epoch,
    );
    let superseded_error = parse_snapshot(&format_snapshot(&superseded_forgery)).unwrap_err();
    assert!(
        superseded_error
            .to_string()
            .contains("invalid staging authorization"),
        "unexpected superseded staging authorization error: {superseded_error}"
    );

    let superseding_transition = superseded
        .snapshot()
        .unavailable_pg_placement_transition(pg_id)
        .unwrap();
    let superseding_binding = UnavailablePgTransitionMutationBinding::new(
        superseding_transition.pg_id,
        superseding_transition.transition_epoch,
        superseding_transition.source_epoch,
        superseding_transition.source_acting_set.clone(),
        superseding_transition.destination_acting_set.clone(),
    );
    let superseding_install = authorize_and_publish_unavailable_transition(
        &mut superseded,
        &superseding_binding,
        proof,
        0x73,
    );
    install_and_complete_unavailable_transition(&mut superseded, &superseding_install);
    let superseding_transition = superseded
        .snapshot()
        .retained_unavailable_pg_placement_transitions
        .get(&(pg_id, superseding_transition_epoch))
        .unwrap();
    let superseding_authorization = superseding_transition
        .staging_authorization
        .as_ref()
        .unwrap();
    let mut superseding_tombstones = superseding_transition
        .destination_acting_set
        .iter()
        .copied()
        .map(|node_id| {
            let node = superseded.snapshot().node(node_id).unwrap();
            MetadataTransferStagingTombstoneBinding {
                node_id,
                node_incarnation: node.node_incarnation(),
                endpoint: node.endpoint().to_owned(),
                evidence_digest: [0x74; 32],
            }
        })
        .collect::<Vec<_>>();
    superseding_tombstones.sort_by_key(|tombstone| tombstone.node_id);
    let missing_tombstone_error = superseded
        .snapshot()
        .apply_control_plane_command(
            ControlPlaneCommand::FinalizeMetadataTransferStagingGeneration {
                cleanup: FinalizeMetadataTransferStagingGenerationRequest {
                    unavailable_transition: superseding_binding,
                    staging_generation: superseding_authorization.staging_generation,
                    disposition: MetadataTransferStagingCleanupDisposition::Completed,
                    tombstones: superseding_tombstones,
                },
            },
        )
        .unwrap_err();
    assert!(
        missing_tombstone_error
            .to_string()
            .contains("lacks a tombstone for node"),
        "unexpected missing-tombstone error: {missing_tombstone_error}"
    );

    let first_install = publish_authorized_unavailable_transition(
        &mut authority,
        first_work.mutation_binding(),
        proof,
        &first_staging_request,
    );
    authority
        .install_unavailable_pg_placement_transitions_batch(
            std::slice::from_ref(&first_install),
            first_install.expected_destination_epoch,
        )
        .unwrap();
    let post_transfer_time = authority.snapshot().max_committed_timestamp_ms().unwrap() + 1;
    for node_id in [4, 2, 3] {
        heartbeat_with_pg_proof_and_lease_duration(
            &mut authority,
            node_id,
            pg_id.get(),
            PgState::Peering,
            proof,
            false,
            (
                post_transfer_time + u64::from(node_id),
                if node_id == 4 { 100 } else { 10_000 },
            ),
        );
    }
    heartbeat_spare_node(&mut authority, 5, post_transfer_time + 10);
    let second_deadline = authority
        .snapshot()
        .node(NodeId::new(4))
        .unwrap()
        .lease_deadline_ms()
        .unwrap();
    authority.expire_heartbeat_leases(second_deadline).unwrap();
    for round in 0..2 {
        for node_id in [2, 3, 5] {
            heartbeat_spare_node(
                &mut authority,
                node_id,
                second_deadline + round * 10 + u64::from(node_id),
            );
        }
    }
    for node_id in [2, 3] {
        heartbeat_with_pg_proof_and_lease_duration(
            &mut authority,
            node_id,
            pg_id.get(),
            PgState::Peering,
            proof,
            false,
            (second_deadline + 30 + u64::from(node_id), 10_000),
        );
    }
    let second_begin_at = (authority
        .snapshot()
        .unavailable_node_observation(NodeId::new(4))
        .unwrap()
        .observed_at_ms()
        + 10)
        .max(authority.snapshot().max_committed_timestamp_ms().unwrap());
    authority
        .begin_unavailable_pg_placement_transition(pg_id, NodeId::new(4), second_begin_at)
        .unwrap();

    let successor = authority
        .snapshot()
        .unavailable_pg_placement_transition(pg_id)
        .unwrap();
    assert_eq!(
        successor.predecessor_transition_epoch(),
        Some(first_transition_epoch)
    );
    assert_eq!(
        successor.destination_acting_set(),
        &[NodeId::new(5), NodeId::new(2), NodeId::new(3)]
    );
    assert_eq!(
        authority
            .snapshot()
            .retained_unavailable_pg_placement_transitions()
            .map(UnavailablePgPlacementTransition::transition_epoch)
            .collect::<Vec<_>>(),
        vec![first_transition_epoch]
    );

    let mut forged_lineage = authority.snapshot().clone();
    forged_lineage
        .unavailable_pg_placement_transitions
        .get_mut(&pg_id)
        .unwrap()
        .predecessor_transition_epoch = None;
    assert!(forged_lineage
        .validate_current_state_invariants()
        .unwrap_err()
        .contains("predecessor"));
}

#[test]
fn unavailable_pg_spare_selection_enforces_domains_and_excludes_draining_nodes() {
    let policy = CertifiedStoragePlacementPolicy::new(
        3,
        0,
        CertifiedStorageFailureDomain::Host,
        0,
        10,
        vec![
            CertifiedStorageNodeDomain::new(NodeId::new(1), "host-a", "disk-1"),
            CertifiedStorageNodeDomain::new(NodeId::new(2), "host-b", "disk-2"),
            CertifiedStorageNodeDomain::new(NodeId::new(3), "host-c", "disk-3"),
            CertifiedStorageNodeDomain::new(NodeId::new(4), "host-b", "disk-4"),
            CertifiedStorageNodeDomain::new(NodeId::new(5), "host-d", "disk-5"),
            CertifiedStorageNodeDomain::new(NodeId::new(6), "host-e", "disk-6"),
        ],
    )
    .unwrap();
    let (_tmp, _store, mut authority, pg_id) = certified_spare_authority_with_policy(6, policy);
    for node_id in 1..=6 {
        heartbeat_spare_node(&mut authority, node_id, 1_000 + u64::from(node_id));
    }
    let proof = PgMetadataProof::current(21, 0x2121, 0x3131);
    for node_id in 1..=3 {
        heartbeat_with_pg_proof_and_lease_duration(
            &mut authority,
            node_id,
            pg_id.get(),
            PgState::Peering,
            proof,
            false,
            (1_100 + u64::from(node_id), 10_000),
        );
    }
    authority
        .complete_pg_peering(
            pg_id,
            NodeId::new(1),
            node_incarnation(&authority, 1),
            1_104,
        )
        .unwrap();
    authority
        .set_node_membership(NodeId::new(5), NodeMembershipState::Draining)
        .unwrap();
    let epoch = authority.snapshot().cluster_epoch();
    for node_id in [1, 2, 3, 4, 6] {
        heartbeat_spare_node(
            &mut authority,
            node_id,
            2_000 + epoch.get() + u64::from(node_id),
        );
    }
    let unavailable = NodeId::new(1);
    let deadline = authority
        .snapshot()
        .node(unavailable)
        .unwrap()
        .lease_deadline_ms()
        .unwrap();
    authority.expire_heartbeat_leases(deadline).unwrap();
    for node_id in [2, 3, 4, 6] {
        heartbeat_spare_node(&mut authority, node_id, deadline + u64::from(node_id));
    }
    for node_id in [2, 3] {
        heartbeat_with_pg_proof_and_lease_duration(
            &mut authority,
            node_id,
            pg_id.get(),
            PgState::Peering,
            proof,
            false,
            (deadline + 10 + u64::from(node_id), 10_000),
        );
    }
    let begin_at = (authority
        .snapshot()
        .unavailable_node_observation(unavailable)
        .unwrap()
        .observed_at_ms()
        + 10)
        .max(authority.snapshot().max_committed_timestamp_ms().unwrap());
    authority
        .begin_unavailable_pg_placement_transition(pg_id, unavailable, begin_at)
        .unwrap();
    assert_eq!(
        authority
            .snapshot()
            .unavailable_pg_placement_transition(pg_id)
            .unwrap()
            .destination_acting_set(),
        &[NodeId::new(6), NodeId::new(2), NodeId::new(3)]
    );
}

#[test]
fn unavailable_pg_transition_rejects_a_proposal_after_exact_incarnation_renewal() {
    let (_tmp, _store, mut authority, pg_id) = certified_spare_authority();
    for node_id in 1..=4 {
        heartbeat_spare_node(&mut authority, node_id, 2_000 + u64::from(node_id));
    }
    let proof = PgMetadataProof::current(23, 0x2323, 0x3333);
    for node_id in 1..=3 {
        heartbeat_with_pg_proof_and_lease_duration(
            &mut authority,
            node_id,
            pg_id.get(),
            PgState::Peering,
            proof,
            false,
            (2_100 + u64::from(node_id), 10_000),
        );
    }
    authority
        .complete_pg_peering(
            pg_id,
            NodeId::new(1),
            node_incarnation(&authority, 1),
            2_104,
        )
        .unwrap();
    let failed_deadline = authority
        .snapshot()
        .node(NodeId::new(1))
        .unwrap()
        .lease_deadline_ms()
        .unwrap();
    authority.expire_heartbeat_leases(failed_deadline).unwrap();
    for node_id in 2..=4 {
        heartbeat_spare_node(
            &mut authority,
            node_id,
            failed_deadline + u64::from(node_id),
        );
    }
    for node_id in [2, 3] {
        heartbeat_with_pg_proof_and_lease_duration(
            &mut authority,
            node_id,
            pg_id.get(),
            PgState::Peering,
            proof,
            false,
            (failed_deadline + 10 + u64::from(node_id), 10_000),
        );
    }
    let observed_at_ms = authority
        .snapshot()
        .unavailable_node_observation(NodeId::new(1))
        .unwrap()
        .observed_at_ms();
    let proposal_at_ms =
        (observed_at_ms + 1).max(authority.snapshot().max_committed_timestamp_ms().unwrap());
    let proposal = authority
        .snapshot()
        .begin_unavailable_pg_placement_transition_command(pg_id, NodeId::new(1), proposal_at_ms)
        .unwrap();

    heartbeat_spare_node(&mut authority, 1, observed_at_ms + 2);
    let renewed = authority.snapshot().clone();
    assert!(renewed
        .unavailable_node_observation(NodeId::new(1))
        .is_none());
    let renewed_pg = renewed.pg(pg_id).unwrap().clone();
    assert!(renewed.apply_control_plane_command(proposal).is_err());
    assert_eq!(renewed.pg(pg_id).unwrap(), &renewed_pg);
    assert!(authority
        .snapshot()
        .unavailable_pg_placement_transition(pg_id)
        .is_none());
}

#[test]
fn administrative_unavailable_fence_survives_heartbeat_and_restart() {
    let tmp = test_util::tempdir();
    let store = FileControlPlaneStore::new(tmp.path().join("control-plane.state"));
    let mut authority = SingleAuthorityControlPlane::open(store.clone()).unwrap();
    authority
        .set_node_membership(NodeId::new(2), NodeMembershipState::Active)
        .unwrap();
    let healthy_epoch = authority
        .heartbeat(heartbeat(2, authority.snapshot().cluster_epoch(), 100), 100)
        .unwrap()
        .cluster_epoch();

    authority
        .mark_node_availability(NodeId::new(2), NodeAvailabilityState::Unavailable)
        .unwrap();
    let unavailable = authority.snapshot().node(NodeId::new(2)).unwrap();
    assert_eq!(unavailable.membership(), NodeMembershipState::Active);
    assert_eq!(
        unavailable.availability(),
        NodeAvailabilityState::Unavailable
    );
    assert!(!unavailable.administratively_available());
    assert_eq!(
        unavailable.observed_availability(),
        NodeAvailabilityState::Unavailable
    );
    assert!(authority.snapshot().cluster_epoch() > healthy_epoch);

    let unavailable_epoch = authority.snapshot().cluster_epoch();
    let fenced = authority
        .heartbeat(heartbeat(2, unavailable_epoch, 500), 500)
        .unwrap();
    assert_eq!(fenced.cluster_epoch(), unavailable_epoch);
    assert!(!fenced.serving());
    let fenced_node = authority.snapshot().node(NodeId::new(2)).unwrap();
    assert!(!fenced_node.administratively_available());
    assert_eq!(
        fenced_node.observed_availability(),
        NodeAvailabilityState::Healthy
    );
    let fenced_lease_deadline_ms = fenced_node.lease_deadline_ms();

    authority
        .mark_node_availability(NodeId::new(2), NodeAvailabilityState::Unavailable)
        .unwrap();
    let still_fenced = authority.snapshot().node(NodeId::new(2)).unwrap();
    assert_eq!(authority.snapshot().cluster_epoch(), unavailable_epoch);
    assert_eq!(
        still_fenced.observed_availability(),
        NodeAvailabilityState::Healthy
    );
    assert_eq!(still_fenced.lease_deadline_ms(), fenced_lease_deadline_ms);

    let mut authority = reopen_file_authority(&store);
    let restarted = authority.snapshot().node(NodeId::new(2)).unwrap();
    assert!(!restarted.administratively_available());
    assert_eq!(restarted.availability(), NodeAvailabilityState::Unavailable);
    assert_eq!(
        restarted.observed_availability(),
        NodeAvailabilityState::Healthy
    );
    let restart_epoch = authority.snapshot().cluster_epoch();
    assert!(!authority
        .heartbeat(heartbeat(2, restart_epoch, 600), 600)
        .unwrap()
        .serving());
    let expiry = authority.expire_heartbeat_leases(700).unwrap();
    assert_eq!(expiry.cluster_epoch(), restart_epoch);
    assert_eq!(expiry.expired_nodes(), &[NodeId::new(2)]);
    assert!(expiry.peering_pgs().is_empty());
    let expired_fenced = authority.snapshot().node(NodeId::new(2)).unwrap();
    assert!(!expired_fenced.administratively_available());
    assert_eq!(
        expired_fenced.observed_availability(),
        NodeAvailabilityState::Unavailable
    );

    authority
        .mark_node_availability(NodeId::new(2), NodeAvailabilityState::Suspect)
        .unwrap();
    let enabled_epoch = authority.snapshot().cluster_epoch();
    let recovering = authority
        .heartbeat(heartbeat(2, enabled_epoch, 700), 700)
        .unwrap();
    assert!(!recovering.serving());
    let serving = authority
        .heartbeat(heartbeat(2, recovering.cluster_epoch(), 800), 800)
        .unwrap();
    assert!(serving.serving());
    let enabled = authority.snapshot().node(NodeId::new(2)).unwrap();
    assert_eq!(enabled.membership(), NodeMembershipState::Active);
    assert!(enabled.administratively_available());
    assert_eq!(
        enabled.observed_availability(),
        NodeAvailabilityState::Healthy
    );
}

#[test]
fn deterministic_primary_uses_first_healthy_serving_acting_set_member() {
    let tmp = test_util::tempdir();
    let store = FileControlPlaneStore::new(tmp.path().join("control-plane.state"));
    let mut authority = SingleAuthorityControlPlane::open(store).unwrap();
    for node_id in [1, 2, 3] {
        authority
            .set_node_membership(NodeId::new(node_id), NodeMembershipState::Active)
            .unwrap();
        assert!(heartbeat_until_serving(&mut authority, node_id, 1_000).serving());
    }
    authority
        .mark_node_availability(NodeId::new(1), NodeAvailabilityState::Unavailable)
        .unwrap();
    authority
        .set_node_membership(NodeId::new(2), NodeMembershipState::Out)
        .unwrap();
    authority
        .heartbeat(
            heartbeat_from_record(&authority, 3, authority.snapshot().cluster_epoch(), 2_000),
            2_000,
        )
        .unwrap();

    let acting_set = [NodeId::new(1), NodeId::new(2), NodeId::new(3)];
    assert_eq!(
        authority.deterministic_pg_primary(PgId::new(7), &acting_set, 2_000),
        Some(NodeId::new(3))
    );
}

#[test]
fn expired_heartbeat_lease_marks_node_unavailable_and_bumps_epoch_once() {
    let tmp = test_util::tempdir();
    let store_path = tmp.path().join("control-plane.state");
    let store = FileControlPlaneStore::new(&store_path);
    let mut authority = SingleAuthorityControlPlane::open(store.clone()).unwrap();
    authority
        .set_node_membership(NodeId::new(1), NodeMembershipState::Active)
        .unwrap();
    authority
        .set_node_membership(NodeId::new(2), NodeMembershipState::Active)
        .unwrap();
    let node_one = heartbeat_until_serving(&mut authority, 1, 1_000);
    let node_two = heartbeat_until_serving(&mut authority, 2, 1_000);
    assert!(node_one.serving());
    assert!(node_two.serving());
    let node_one_current = authority
        .heartbeat(
            heartbeat_from_record(&authority, 1, authority.snapshot().cluster_epoch(), 1_002),
            1_002,
        )
        .unwrap();
    assert!(node_one_current.serving());
    assert_eq!(
        authority.deterministic_pg_primary(PgId::new(1), &[NodeId::new(1), NodeId::new(2)], 1_002,),
        Some(NodeId::new(1))
    );
    authority
        .set_pg_acting_set(PgId::new(9), vec![NodeId::new(1), NodeId::new(2)])
        .unwrap();
    for node_id in [1, 2] {
        heartbeat_with_pg_observation(
            &mut authority,
            node_id,
            9,
            PgState::Peering,
            1_003 + u64::from(node_id),
        );
    }
    authority
        .complete_pg_peering(
            PgId::new(9),
            NodeId::new(1),
            node_incarnation(&authority, 1),
            1_050,
        )
        .unwrap();
    heartbeat_with_pg_observation(&mut authority, 1, 9, PgState::Active, 1_011);
    authority
        .heartbeat(
            heartbeat_from_record(&authority, 2, authority.snapshot().cluster_epoch(), 1_051),
            1_051,
        )
        .unwrap();
    assert_eq!(
        authority.serving_pg_primary(PgId::new(9), 1_051),
        Some(NodeId::new(1))
    );

    let before_expiry_epoch = authority.snapshot().cluster_epoch();
    let expiry = authority.expire_heartbeat_leases(1_152).unwrap();
    assert_eq!(expiry.expired_nodes(), &[NodeId::new(1), NodeId::new(2)]);
    assert_eq!(expiry.peering_pgs(), &[PgId::new(9)]);
    assert!(expiry.cluster_epoch() > before_expiry_epoch);
    assert_eq!(expiry.snapshot().cluster_epoch(), expiry.cluster_epoch());
    assert_eq!(
        expiry.snapshot().pg(PgId::new(9)).unwrap().state(),
        PgState::Peering
    );
    assert_eq!(
        authority
            .snapshot()
            .node(NodeId::new(1))
            .unwrap()
            .availability(),
        NodeAvailabilityState::Unavailable
    );
    assert_eq!(
        authority
            .snapshot()
            .node(NodeId::new(1))
            .unwrap()
            .lease_deadline_ms(),
        None
    );
    assert_eq!(
        authority.deterministic_pg_primary(PgId::new(1), &[NodeId::new(1), NodeId::new(2)], 1_112,),
        None
    );
    assert_eq!(authority.serving_pg_primary(PgId::new(9), 1_112), None);

    let durable_after_expiry = std::fs::read(&store_path).unwrap();
    let repeated = authority.expire_heartbeat_leases(9_999).unwrap();
    assert_eq!(repeated.expired_nodes(), &[]);
    assert_eq!(repeated.peering_pgs(), &[]);
    assert_eq!(repeated.cluster_epoch(), expiry.cluster_epoch());
    assert_eq!(
        std::fs::read(&store_path).unwrap(),
        durable_after_expiry,
        "an expiry scan with no lease transition must not rewrite state"
    );

    let persisted = store.load().unwrap().unwrap();
    assert_eq!(persisted.cluster_epoch(), expiry.cluster_epoch());
    assert_eq!(
        persisted.pg(PgId::new(9)).unwrap().state(),
        PgState::Peering
    );
    assert_eq!(
        persisted.node(NodeId::new(2)).unwrap().availability(),
        NodeAvailabilityState::Unavailable
    );
}

#[test]
fn heartbeat_after_expiry_must_observe_new_epoch_before_serving() {
    let tmp = test_util::tempdir();
    let store = FileControlPlaneStore::new(tmp.path().join("control-plane.state"));
    let mut authority = SingleAuthorityControlPlane::open(store).unwrap();
    authority
        .set_node_membership(NodeId::new(3), NodeMembershipState::Active)
        .unwrap();
    let serving = heartbeat_until_serving(&mut authority, 3, 1_000);
    assert!(serving.serving());

    let expiry = authority.expire_heartbeat_leases(1_101).unwrap();
    assert_eq!(expiry.expired_nodes(), &[NodeId::new(3)]);

    let stale_after_expiry = authority
        .heartbeat(
            heartbeat_from_record(&authority, 3, serving.cluster_epoch(), 1_200),
            1_200,
        )
        .unwrap();
    assert!(!stale_after_expiry.serving());
    assert_eq!(stale_after_expiry.cluster_epoch(), expiry.cluster_epoch());
    assert_eq!(
        authority
            .snapshot()
            .node(NodeId::new(3))
            .unwrap()
            .availability(),
        NodeAvailabilityState::Unavailable
    );

    let recovered = authority
        .heartbeat(
            heartbeat_from_record(&authority, 3, stale_after_expiry.cluster_epoch(), 1_300),
            1_300,
        )
        .unwrap();
    assert!(recovered.cluster_epoch() > stale_after_expiry.cluster_epoch());
    assert!(!recovered.serving());

    let caught_up = authority
        .heartbeat(
            heartbeat_from_record(&authority, 3, recovered.cluster_epoch(), 1_400),
            1_400,
        )
        .unwrap();
    assert!(caught_up.serving());
    assert_eq!(
        authority.deterministic_pg_primary(PgId::new(1), &[NodeId::new(3)], 1_400),
        Some(NodeId::new(3))
    );
}

#[test]
fn recovered_earlier_primary_forces_active_pg_back_to_peering() {
    let tmp = test_util::tempdir();
    let store = FileControlPlaneStore::new(tmp.path().join("control-plane.state"));
    let mut authority = SingleAuthorityControlPlane::open(store).unwrap();
    for node_id in [1, 2] {
        authority
            .set_node_membership(NodeId::new(node_id), NodeMembershipState::Active)
            .unwrap();
        assert!(heartbeat_until_serving(&mut authority, node_id, 100).serving());
    }
    authority
        .set_pg_acting_set(PgId::new(13), vec![NodeId::new(1), NodeId::new(2)])
        .unwrap();
    heartbeat_with_pg_observation(&mut authority, 1, 13, PgState::Peering, 1_000);
    heartbeat_with_pg_observation(&mut authority, 2, 13, PgState::Peering, 1_050);
    authority
        .complete_pg_peering(
            PgId::new(13),
            NodeId::new(1),
            node_incarnation(&authority, 1),
            1_060,
        )
        .unwrap();
    heartbeat_with_pg_observation(&mut authority, 1, 13, PgState::Active, 1_070);
    authority
        .heartbeat(
            heartbeat_from_record(&authority, 2, authority.snapshot().cluster_epoch(), 1_080),
            1_080,
        )
        .unwrap();
    assert_eq!(
        authority.serving_pg_primary(PgId::new(13), 1_080),
        Some(NodeId::new(1))
    );

    let node_one_deadline = authority
        .snapshot()
        .node(NodeId::new(1))
        .unwrap()
        .lease_deadline_ms()
        .unwrap();
    let expiry = authority
        .expire_heartbeat_leases(node_one_deadline)
        .unwrap();
    assert_eq!(expiry.expired_nodes(), &[NodeId::new(1)]);
    assert_eq!(expiry.peering_pgs(), &[PgId::new(13)]);

    heartbeat_with_pg_observation(
        &mut authority,
        2,
        13,
        PgState::Peering,
        node_one_deadline + 1,
    );
    let successor_fence_ms = node_one_deadline + CONTROL_PLANE_CLOCK_SKEW_BUDGET_MS;
    heartbeat_with_pg_observation(&mut authority, 2, 13, PgState::Peering, successor_fence_ms);
    authority
        .complete_pg_peering(
            PgId::new(13),
            NodeId::new(2),
            node_incarnation(&authority, 2),
            successor_fence_ms,
        )
        .unwrap();
    heartbeat_with_pg_observation(
        &mut authority,
        2,
        13,
        PgState::Active,
        successor_fence_ms + 1,
    );
    assert_eq!(
        authority.serving_pg_primary(PgId::new(13), successor_fence_ms + 1),
        Some(NodeId::new(2))
    );

    let recovered = authority
        .heartbeat(
            heartbeat_from_record(
                &authority,
                1,
                authority.snapshot().cluster_epoch(),
                successor_fence_ms + 2,
            ),
            successor_fence_ms + 2,
        )
        .unwrap();
    assert!(!recovered.serving());
    assert_eq!(
        authority.snapshot().pg(PgId::new(13)).unwrap().state(),
        PgState::Peering
    );
    assert_eq!(
        authority.serving_pg_primary(PgId::new(13), successor_fence_ms + 2),
        None
    );

    let caught_up = authority
        .heartbeat(
            heartbeat_from_record(
                &authority,
                1,
                recovered.cluster_epoch(),
                successor_fence_ms + 3,
            ),
            successor_fence_ms + 3,
        )
        .unwrap();
    assert!(caught_up.serving());
    assert_eq!(
        authority.snapshot().pg(PgId::new(13)).unwrap().state(),
        PgState::Peering
    );
    assert!(matches!(
        authority.authorize_pg_primary_service(
            PgId::new(13),
            NodeId::new(1),
            node_incarnation(&authority, 1),
            authority.snapshot().cluster_epoch(),
            successor_fence_ms + 4,
        ),
        Err(ControlPlaneError::PgNotActive { pg_id: 13, .. })
    ));
}

#[test]
fn stale_runtime_map_fails_closed_after_epoch_transition() {
    let tmp = test_util::tempdir();
    let store = FileControlPlaneStore::new(tmp.path().join("control-plane.state"));
    let mut authority = SingleAuthorityControlPlane::open(store).unwrap();
    authority
        .set_node_membership(NodeId::new(1), NodeMembershipState::Active)
        .unwrap();
    assert!(heartbeat_until_serving(&mut authority, 1, 1_000).serving());
    authority
        .set_pg_acting_set(PgId::new(17), vec![NodeId::new(1)])
        .unwrap();
    heartbeat_with_pg_observation(&mut authority, 1, 17, PgState::Peering, 2_000);
    authority
        .complete_pg_peering(
            PgId::new(17),
            NodeId::new(1),
            node_incarnation(&authority, 1),
            2_001,
        )
        .unwrap();
    heartbeat_with_pg_observation(&mut authority, 1, 17, PgState::Active, 2_002);

    let active_map = authority.snapshot().runtime_map(2_003).unwrap();
    let stale_frontend_cluster = crate::StorageCluster::from_runtime_map(
        NodeId::new(1),
        &active_map,
        crate::EcShape { k: 1, m: 0 },
    )
    .unwrap();
    let valid_until_ms = active_map.valid_until_ms().unwrap();
    assert_eq!(
        stale_frontend_cluster.route_map_valid_until_ms(),
        Some(valid_until_ms)
    );
    assert!(stale_frontend_cluster
        .require_route_map_valid_at(valid_until_ms - 1)
        .is_ok());

    let before_expiry_epoch = authority.snapshot().cluster_epoch();
    let expiry = authority.expire_heartbeat_leases(valid_until_ms).unwrap();
    assert_eq!(expiry.expired_nodes(), &[NodeId::new(1)]);
    assert!(expiry.cluster_epoch() > before_expiry_epoch);
    assert_eq!(
        authority.snapshot().pg(PgId::new(17)).unwrap().state(),
        PgState::Peering
    );

    assert!(matches!(
        stale_frontend_cluster.require_route_map_valid_at(valid_until_ms),
        Err(crate::StoreError::RouteMapExpired {
            cluster_epoch,
            valid_until_ms: expired_at,
            now_ms,
        }) if cluster_epoch == active_map.cluster_epoch()
            && expired_at == valid_until_ms
            && now_ms == valid_until_ms
    ));
    assert_eq!(
        authority
            .snapshot()
            .runtime_map(valid_until_ms)
            .unwrap()
            .pg_routes()[0]
            .state(),
        PgState::Peering
    );
}

#[test]
fn stale_primary_authorization_cannot_validate_after_epoch_transition() {
    let tmp = test_util::tempdir();
    let store = FileControlPlaneStore::new(tmp.path().join("control-plane.state"));
    let mut authority = SingleAuthorityControlPlane::open(store).unwrap();
    authority
        .set_node_membership(NodeId::new(1), NodeMembershipState::Active)
        .unwrap();
    assert!(heartbeat_until_serving(&mut authority, 1, 1_000).serving());
    authority
        .set_pg_acting_set(PgId::new(18), vec![NodeId::new(1)])
        .unwrap();
    heartbeat_with_pg_observation(&mut authority, 1, 18, PgState::Peering, 2_000);
    authority
        .complete_pg_peering(
            PgId::new(18),
            NodeId::new(1),
            node_incarnation(&authority, 1),
            2_001,
        )
        .unwrap();
    let active = heartbeat_with_pg_observation(&mut authority, 1, 18, PgState::Active, 2_002);
    let authorization = authority
        .authorize_pg_operation(
            PgServiceOperation::MetadataWrite,
            PgId::new(18),
            NodeId::new(1),
            node_incarnation(&authority, 1),
            active.cluster_epoch(),
            2_003,
        )
        .unwrap();
    assert!(authority
        .validate_pg_operation_authorization(&authorization, 2_004)
        .is_ok());

    let lease_deadline_ms = authorization.primary().lease_deadline_ms();
    let expiry = authority
        .expire_heartbeat_leases(lease_deadline_ms)
        .unwrap();
    assert_eq!(expiry.expired_nodes(), &[NodeId::new(1)]);
    assert_eq!(
        authority.snapshot().pg(PgId::new(18)).unwrap().state(),
        PgState::Peering
    );

    assert!(matches!(
        authority.validate_pg_operation_authorization(&authorization, lease_deadline_ms),
        Err(ControlPlaneError::StaleAuthorizationEpoch {
            cluster_epoch,
            current_epoch,
        }) if cluster_epoch == active.cluster_epoch()
            && current_epoch == expiry.cluster_epoch()
    ));
    assert!(matches!(
        authority.authorize_pg_primary_service(
            PgId::new(18),
            NodeId::new(1),
            node_incarnation(&authority, 1),
            expiry.cluster_epoch(),
            lease_deadline_ms,
        ),
        Err(ControlPlaneError::NodeLeaseExpired { node_id: 1, .. })
    ));

    let recovery = authority
        .heartbeat(
            heartbeat_from_record(&authority, 1, expiry.cluster_epoch(), lease_deadline_ms + 1),
            lease_deadline_ms + 1,
        )
        .unwrap();
    assert!(
        !recovery.serving(),
        "availability recovery bumps the epoch before the node observes it"
    );
    let recovery_epoch = recovery.cluster_epoch();
    assert!(recovery_epoch > expiry.cluster_epoch());

    let caught_up = authority
        .heartbeat(
            heartbeat_from_record(&authority, 1, recovery_epoch, lease_deadline_ms + 2),
            lease_deadline_ms + 2,
        )
        .unwrap();
    assert!(caught_up.serving());
    assert!(matches!(
        authority.authorize_pg_operation(
            PgServiceOperation::MetadataWrite,
            PgId::new(18),
            NodeId::new(1),
            node_incarnation(&authority, 1),
            recovery_epoch,
            lease_deadline_ms + 3,
        ),
        Err(ControlPlaneError::PgNotActive {
            pg_id: 18,
            state: PgState::Peering,
            ..
        })
    ));

    heartbeat_with_pg_observation(
        &mut authority,
        1,
        18,
        PgState::Peering,
        lease_deadline_ms + 4,
    );
    authority
        .complete_pg_peering(
            PgId::new(18),
            NodeId::new(1),
            node_incarnation(&authority, 1),
            lease_deadline_ms + 5,
        )
        .unwrap();
    let active_again = heartbeat_with_pg_observation(
        &mut authority,
        1,
        18,
        PgState::Active,
        lease_deadline_ms + 6,
    );
    let fresh_authorization = authority
        .authorize_pg_operation(
            PgServiceOperation::MetadataWrite,
            PgId::new(18),
            NodeId::new(1),
            node_incarnation(&authority, 1),
            active_again.cluster_epoch(),
            lease_deadline_ms + 7,
        )
        .unwrap();
    assert_eq!(
        fresh_authorization.cluster_epoch(),
        active_again.cluster_epoch()
    );
    authority
        .validate_pg_operation_authorization(&fresh_authorization, lease_deadline_ms + 8)
        .unwrap();
}

#[test]
fn storage_node_refresh_after_epoch_transition_cannot_keep_stale_active_route() {
    let tmp = test_util::tempdir();
    let store = FileControlPlaneStore::new(tmp.path().join("control-plane.state"));
    let mut authority = SingleAuthorityControlPlane::open(store).unwrap();
    authority
        .set_node_membership(NodeId::new(1), NodeMembershipState::Active)
        .unwrap();
    assert!(heartbeat_until_serving(&mut authority, 1, 1_000).serving());
    authority
        .set_pg_acting_set(PgId::new(19), vec![NodeId::new(1)])
        .unwrap();
    heartbeat_with_pg_observation(&mut authority, 1, 19, PgState::Peering, 2_000);
    authority
        .complete_pg_peering(
            PgId::new(19),
            NodeId::new(1),
            node_incarnation(&authority, 1),
            2_001,
        )
        .unwrap();
    let active = heartbeat_with_pg_observation(&mut authority, 1, 19, PgState::Active, 2_002);
    let active_epoch = active.cluster_epoch();
    let active_map = authority.snapshot().runtime_map(2_003).unwrap();
    let active_valid_until_ms = active_map.valid_until_ms().unwrap();
    assert_eq!(active_map.pg_routes()[0].state(), PgState::Active);

    let expiry = authority
        .expire_heartbeat_leases(active_valid_until_ms)
        .unwrap();
    assert_eq!(expiry.expired_nodes(), &[NodeId::new(1)]);
    assert!(expiry.cluster_epoch() > active_epoch);
    assert_eq!(
        authority.snapshot().pg(PgId::new(19)).unwrap().state(),
        PgState::Peering
    );

    let mut stale_active_heartbeat =
        heartbeat_from_record(&authority, 1, active_epoch, active_valid_until_ms + 1);
    stale_active_heartbeat.pg_observations = vec![NodePgHeartbeatObservation {
        pg_id: PgId::new(19),
        state: PgState::Active,
        metadata_proof: PgMetadataProof::empty(),
        metadata_log_epoch: ClusterEpoch::INITIAL,
        pending_metadata_command: None,
    }];
    let refresh = authority
        .refresh_node_heartbeat(stale_active_heartbeat, active_valid_until_ms + 1)
        .unwrap();

    assert!(!refresh.lease().serving());
    assert_eq!(refresh.lease().cluster_epoch(), expiry.cluster_epoch());
    assert_eq!(
        refresh.runtime_map().cluster_epoch(),
        expiry.cluster_epoch()
    );
    assert_eq!(
        refresh.runtime_map().pg_routes()[0].state(),
        PgState::Peering
    );
    assert_eq!(
        refresh.runtime_map().pg_routes()[0].primary_lease_deadline_ms(),
        None
    );
    let record = authority.snapshot().node(NodeId::new(1)).unwrap();
    assert_eq!(record.availability(), NodeAvailabilityState::Unavailable);
    assert_eq!(record.last_observed_epoch(), Some(active_epoch));
}

#[test]
fn temporary_availability_loss_reactivates_same_primary_before_old_lease_expires() {
    let tmp = test_util::tempdir();
    let store = FileControlPlaneStore::new(tmp.path().join("control-plane.state"));
    let mut authority = SingleAuthorityControlPlane::open(store).unwrap();
    for node_id in [1, 2] {
        authority
            .set_node_membership(NodeId::new(node_id), NodeMembershipState::Active)
            .unwrap();
        assert!(heartbeat_until_serving(&mut authority, node_id, 1_000).serving());
    }
    authority
        .set_pg_acting_set(PgId::new(25), vec![NodeId::new(1), NodeId::new(2)])
        .unwrap();
    heartbeat_with_pg_observation(&mut authority, 1, 25, PgState::Peering, 2_000);
    heartbeat_with_pg_observation(&mut authority, 2, 25, PgState::Peering, 2_001);
    authority
        .complete_pg_peering(
            PgId::new(25),
            NodeId::new(1),
            node_incarnation(&authority, 1),
            2_002,
        )
        .unwrap();
    let active = heartbeat_with_pg_observation(&mut authority, 1, 25, PgState::Active, 2_003);
    let active_epoch = active.cluster_epoch();
    let active_authorization = authority
        .authorize_pg_operation(
            PgServiceOperation::MetadataWrite,
            PgId::new(25),
            NodeId::new(1),
            node_incarnation(&authority, 1),
            active_epoch,
            2_004,
        )
        .unwrap();

    authority
        .mark_node_availability(NodeId::new(1), NodeAvailabilityState::Suspect)
        .unwrap();
    let suspect_epoch = authority.snapshot().cluster_epoch();
    assert!(suspect_epoch > active_epoch);
    let pg = authority.snapshot().pg(PgId::new(25)).unwrap();
    assert_eq!(pg.state(), PgState::Peering);
    assert_eq!(pg.active_primary(), None);
    assert_eq!(pg.active_metadata_proof(), None);
    assert_eq!(
        pg.peering_metadata_proof_floor(),
        Some(PgMetadataProof::empty())
    );
    assert!(matches!(
        authority.validate_pg_operation_authorization(&active_authorization, 2_005),
        Err(ControlPlaneError::StaleAuthorizationEpoch {
            cluster_epoch,
            current_epoch,
        }) if cluster_epoch == active_epoch && current_epoch == suspect_epoch
    ));
    assert!(matches!(
        authority.authorize_pg_operation(
            PgServiceOperation::MetadataWrite,
            PgId::new(25),
            NodeId::new(1),
            node_incarnation(&authority, 1),
            suspect_epoch,
            2_006,
        ),
        Err(ControlPlaneError::NodeLeaseExpired { node_id: 1, .. })
    ));

    let node_two = authority
        .heartbeat(
            heartbeat_from_record(&authority, 2, suspect_epoch, 2_007),
            2_007,
        )
        .unwrap();
    assert!(node_two.serving());
    assert!(matches!(
        authority.authorize_pg_operation(
            PgServiceOperation::MetadataWrite,
            PgId::new(25),
            NodeId::new(2),
            node_incarnation(&authority, 2),
            suspect_epoch,
            2_008,
        ),
        Err(ControlPlaneError::PgNotActive {
            pg_id: 25,
            state: PgState::Peering,
            ..
        })
    ));

    let recovery = authority
        .heartbeat(
            heartbeat_from_record(&authority, 1, suspect_epoch, 2_009),
            2_009,
        )
        .unwrap();
    assert!(
        !recovery.serving(),
        "availability recovery bumps the epoch before the node observes it"
    );
    let recovery_epoch = recovery.cluster_epoch();
    assert!(recovery_epoch > suspect_epoch);
    assert_eq!(
        authority.snapshot().pg(PgId::new(25)).unwrap().state(),
        PgState::Peering
    );

    assert!(authority
        .heartbeat(
            heartbeat_from_record(&authority, 1, recovery_epoch, 2_010),
            2_010,
        )
        .unwrap()
        .serving());
    heartbeat_with_pg_observation(&mut authority, 1, 25, PgState::Peering, 2_011);
    heartbeat_with_pg_observation(&mut authority, 2, 25, PgState::Peering, 2_012);
    let pg = authority.snapshot().pg(PgId::new(25)).unwrap();
    assert_eq!(pg.previous_primary_node_id(), Some(NodeId::new(1)));
    assert_eq!(
        pg.previous_primary_node_incarnation(),
        Some(node_incarnation(&authority, 1))
    );
    assert!(pg.previous_primary_lease_deadline_ms().unwrap() > 2_013);
    assert_eq!(
        authority.complete_ready_pg_peerings(2_013).unwrap(),
        vec![PgId::new(25)],
        "the unchanged primary process must not wait out its own old lease"
    );
    let active_again = heartbeat_with_pg_observation(&mut authority, 1, 25, PgState::Active, 2_014);
    let authorization = authority
        .authorize_pg_operation(
            PgServiceOperation::MetadataWrite,
            PgId::new(25),
            NodeId::new(1),
            node_incarnation(&authority, 1),
            active_again.cluster_epoch(),
            2_015,
        )
        .unwrap();
    assert_eq!(authorization.primary_node_id(), NodeId::new(1));
}

#[test]
fn recovering_preferred_replica_does_not_displace_live_previous_primary() {
    let tmp = test_util::tempdir();
    let store = FileControlPlaneStore::new(tmp.path().join("control-plane.state"));
    let mut authority = SingleAuthorityControlPlane::open(store).unwrap();
    for node_id in [1, 2] {
        authority
            .set_node_membership(NodeId::new(node_id), NodeMembershipState::Active)
            .unwrap();
        assert!(heartbeat_until_serving(&mut authority, node_id, 1_000).serving());
    }

    authority
        .mark_node_availability(NodeId::new(2), NodeAvailabilityState::Suspect)
        .unwrap();
    authority
        .set_pg_acting_set(PgId::new(26), vec![NodeId::new(2), NodeId::new(1)])
        .unwrap();
    heartbeat_with_pg_observation(&mut authority, 1, 26, PgState::Peering, 2_000);
    authority
        .complete_pg_peering(
            PgId::new(26),
            NodeId::new(1),
            node_incarnation(&authority, 1),
            2_001,
        )
        .unwrap();
    heartbeat_with_pg_observation(&mut authority, 1, 26, PgState::Active, 2_002);

    let recovery = heartbeat_with_pg_observation(&mut authority, 2, 26, PgState::Peering, 2_003);
    assert!(
        !recovery.serving(),
        "recovering replica must first observe its availability epoch"
    );
    let recovery_epoch = recovery.cluster_epoch();
    let pg = authority.snapshot().pg(PgId::new(26)).unwrap();
    assert_eq!(pg.state(), PgState::Peering);
    assert_eq!(pg.previous_primary_node_id(), Some(NodeId::new(1)));
    assert!(pg.previous_primary_lease_deadline_ms().unwrap() > 2_006);

    heartbeat_with_pg_observation(&mut authority, 1, 26, PgState::Peering, 2_004);
    heartbeat_with_pg_observation(&mut authority, 2, 26, PgState::Peering, 2_005);
    assert_eq!(authority.snapshot().cluster_epoch(), recovery_epoch);
    assert!(matches!(
        authority.complete_pg_peering(
            PgId::new(26),
            NodeId::new(2),
            node_incarnation(&authority, 2),
            2_006,
        ),
        Err(ControlPlaneError::PgPrimaryNotServingCurrentEpoch {
            pg_id: 26,
            node_id: 2,
        })
    ));
    let ready = authority
        .snapshot()
        .ready_pg_peering_completions(2_006)
        .unwrap();
    assert_eq!(ready.len(), 1);
    assert_eq!(
        ready[0].primary,
        NodeId::new(1),
        "the exact previous primary must retain priority while its old lease is live"
    );
    assert_eq!(
        authority.complete_ready_pg_peerings(2_006).unwrap(),
        vec![PgId::new(26)]
    );
    assert_eq!(
        authority
            .snapshot()
            .pg(PgId::new(26))
            .unwrap()
            .active_primary(),
        Some(NodeId::new(1))
    );
}

#[test]
fn acting_set_reorder_moves_primary_after_previous_lease_expires() {
    let tmp = test_util::tempdir();
    let store = FileControlPlaneStore::new(tmp.path().join("control-plane.state"));
    let mut authority = SingleAuthorityControlPlane::open(store.clone()).unwrap();
    for node_id in [1, 2] {
        authority
            .set_node_membership(NodeId::new(node_id), NodeMembershipState::Active)
            .unwrap();
        assert!(heartbeat_until_serving(&mut authority, node_id, 1_000).serving());
    }

    authority
        .set_pg_acting_set(PgId::new(27), vec![NodeId::new(1), NodeId::new(2)])
        .unwrap();
    heartbeat_with_pg_observation(&mut authority, 1, 27, PgState::Peering, 2_000);
    heartbeat_with_pg_observation(&mut authority, 2, 27, PgState::Peering, 2_001);
    authority
        .complete_pg_peering(
            PgId::new(27),
            NodeId::new(1),
            node_incarnation(&authority, 1),
            2_002,
        )
        .unwrap();
    heartbeat_with_pg_observation(&mut authority, 1, 27, PgState::Active, 2_003);
    heartbeat_with_pg_observation(&mut authority, 2, 27, PgState::Active, 2_004);

    authority
        .set_pg_acting_set(PgId::new(27), vec![NodeId::new(2), NodeId::new(1)])
        .unwrap();
    let reordered_epoch = authority.snapshot().cluster_epoch();
    let previous_lease_deadline = authority
        .snapshot()
        .pg(PgId::new(27))
        .unwrap()
        .previous_primary_lease_deadline_ms()
        .unwrap();
    assert_eq!(
        authority
            .snapshot()
            .pg(PgId::new(27))
            .unwrap()
            .previous_primary_lease
            .as_ref()
            .map(|previous| previous.prefer_reactivation),
        Some(false)
    );

    drop(authority);
    let mut authority = SingleAuthorityControlPlane::open(store).unwrap();
    let peering_epoch = authority.snapshot().cluster_epoch();
    assert!(peering_epoch > reordered_epoch);
    let pg = authority.snapshot().pg(PgId::new(27)).unwrap();
    assert_eq!(
        pg.previous_primary_lease_deadline_ms(),
        Some(previous_lease_deadline)
    );
    assert_eq!(
        pg.previous_primary_lease
            .as_ref()
            .map(|previous| previous.prefer_reactivation),
        Some(false),
        "acting-set transition provenance must survive authority restart"
    );
    heartbeat_with_pg_observation(&mut authority, 1, 27, PgState::Peering, 2_005);
    heartbeat_with_pg_observation(&mut authority, 2, 27, PgState::Peering, 2_006);
    assert_eq!(authority.snapshot().cluster_epoch(), peering_epoch);

    assert!(authority
        .snapshot()
        .ready_pg_peering_completions(previous_lease_deadline - 1)
        .unwrap()
        .is_empty());
    assert!(matches!(
        authority.complete_pg_peering(
            PgId::new(27),
            NodeId::new(1),
            node_incarnation(&authority, 1),
            previous_lease_deadline - 1,
        ),
        Err(ControlPlaneError::PgPrimaryNotServingCurrentEpoch {
            pg_id: 27,
            node_id: 1,
        })
    ));
    assert!(matches!(
        authority.complete_pg_peering(
            PgId::new(27),
            NodeId::new(2),
            node_incarnation(&authority, 2),
            previous_lease_deadline - 1,
        ),
        Err(ControlPlaneError::PgPreviousPrimaryLeaseStillActive { pg_id: 27, .. })
    ));

    assert!(authority
        .snapshot()
        .ready_pg_peering_completions(previous_lease_deadline)
        .unwrap()
        .is_empty());
    heartbeat_with_pg_observation(
        &mut authority,
        1,
        27,
        PgState::Peering,
        previous_lease_deadline,
    );
    heartbeat_with_pg_observation(
        &mut authority,
        2,
        27,
        PgState::Peering,
        previous_lease_deadline + 1,
    );
    let successor_fence_ms = previous_lease_deadline + CONTROL_PLANE_CLOCK_SKEW_BUDGET_MS;
    heartbeat_with_pg_observation(&mut authority, 1, 27, PgState::Peering, successor_fence_ms);
    heartbeat_with_pg_observation(
        &mut authority,
        2,
        27,
        PgState::Peering,
        successor_fence_ms + 1,
    );
    let ready = authority
        .snapshot()
        .ready_pg_peering_completions(successor_fence_ms + 1)
        .unwrap();
    assert_eq!(ready.len(), 1);
    assert_eq!(ready[0].primary, NodeId::new(2));
    assert_eq!(
        authority
            .complete_ready_pg_peerings(successor_fence_ms + 1)
            .unwrap(),
        vec![PgId::new(27)]
    );
    assert_eq!(
        authority
            .snapshot()
            .pg(PgId::new(27))
            .unwrap()
            .active_primary(),
        Some(NodeId::new(2))
    );
}

#[test]
fn acting_set_change_fences_old_primary_token_until_new_peering_completes() {
    let tmp = test_util::tempdir();
    let store = FileControlPlaneStore::new(tmp.path().join("control-plane.state"));
    let mut authority = SingleAuthorityControlPlane::open(store).unwrap();
    for node_id in [1, 2] {
        authority
            .set_node_membership(NodeId::new(node_id), NodeMembershipState::Active)
            .unwrap();
        assert!(heartbeat_until_serving(&mut authority, node_id, 1_000).serving());
    }
    let active_proof = PgMetadataProof::current(77, 0xabcddcba, 0x12344321);
    authority
        .set_pg_acting_set(PgId::new(20), vec![NodeId::new(1), NodeId::new(2)])
        .unwrap();
    heartbeat_with_pg_proof(
        &mut authority,
        1,
        20,
        PgState::Peering,
        active_proof,
        false,
        2_000,
    );
    heartbeat_with_pg_proof(
        &mut authority,
        2,
        20,
        PgState::Peering,
        active_proof,
        false,
        2_001,
    );
    authority
        .complete_pg_peering(
            PgId::new(20),
            NodeId::new(1),
            node_incarnation(&authority, 1),
            2_002,
        )
        .unwrap();
    let active = heartbeat_with_pg_proof(
        &mut authority,
        1,
        20,
        PgState::Active,
        active_proof,
        false,
        2_003,
    );
    heartbeat_with_pg_proof(
        &mut authority,
        2,
        20,
        PgState::Active,
        active_proof,
        false,
        2_004,
    );
    let old_epoch = active.cluster_epoch();
    let old_authorization = authority
        .authorize_pg_operation(
            PgServiceOperation::MetadataWrite,
            PgId::new(20),
            NodeId::new(1),
            node_incarnation(&authority, 1),
            old_epoch,
            2_005,
        )
        .unwrap();
    authority
        .validate_pg_operation_authorization(&old_authorization, 2_006)
        .unwrap();

    authority
        .set_pg_acting_set(PgId::new(20), vec![NodeId::new(2)])
        .unwrap();
    let peering_epoch = authority.snapshot().cluster_epoch();
    assert!(peering_epoch > old_epoch);
    assert_eq!(
        authority.snapshot().pg(PgId::new(20)).unwrap().state(),
        PgState::Peering
    );
    assert_eq!(
        authority
            .snapshot()
            .pg(PgId::new(20))
            .unwrap()
            .peering_metadata_proof_floor(),
        Some(active_proof)
    );
    assert!(matches!(
        authority.validate_pg_operation_authorization(&old_authorization, 2_007),
        Err(ControlPlaneError::StaleAuthorizationEpoch {
            cluster_epoch,
            current_epoch,
        }) if cluster_epoch == old_epoch && current_epoch == peering_epoch
    ));
    for node_id in [1, 2] {
        let now_ms = 2_008 + u64::from(node_id);
        authority
            .heartbeat(
                heartbeat_from_record(&authority, node_id, peering_epoch, now_ms),
                now_ms,
            )
            .unwrap();
    }
    assert!(matches!(
        authority.authorize_pg_operation(
            PgServiceOperation::MetadataWrite,
            PgId::new(20),
            NodeId::new(1),
            node_incarnation(&authority, 1),
            peering_epoch,
            2_011,
        ),
        Err(ControlPlaneError::PgNotActive {
            pg_id: 20,
            state: PgState::Peering,
            ..
        })
    ));
    assert!(matches!(
        authority.authorize_pg_operation(
            PgServiceOperation::MetadataWrite,
            PgId::new(20),
            NodeId::new(2),
            node_incarnation(&authority, 2),
            peering_epoch,
            2_012,
        ),
        Err(ControlPlaneError::PgNotActive {
            pg_id: 20,
            state: PgState::Peering,
            ..
        })
    ));

    let mut stale_node_two_peering = heartbeat_from_record(&authority, 2, peering_epoch, 2_013);
    stale_node_two_peering.pg_observations = vec![NodePgHeartbeatObservation {
        pg_id: PgId::new(20),
        state: PgState::Peering,
        metadata_proof: PgMetadataProof::empty(),
        metadata_log_epoch: ClusterEpoch::INITIAL,
        pending_metadata_command: None,
    }];
    authority.heartbeat(stale_node_two_peering, 2_013).unwrap();
    assert!(matches!(
        authority.complete_pg_peering(
            PgId::new(20),
            NodeId::new(2),
            node_incarnation(&authority, 2),
            2_014,
        ),
        Err(ControlPlaneError::PgPreviousPrimaryLeaseStillActive {
            pg_id: 20,
            lease_deadline_ms: 2_103,
            ..
        })
    ));
    let mut ready_but_fenced = heartbeat_from_record(&authority, 2, peering_epoch, 2_015);
    ready_but_fenced.pg_observations = vec![NodePgHeartbeatObservation {
        pg_id: PgId::new(20),
        state: PgState::Peering,
        metadata_proof: active_proof,
        metadata_log_epoch: ClusterEpoch::INITIAL,
        pending_metadata_command: None,
    }];
    authority.heartbeat(ready_but_fenced, 2_015).unwrap();
    assert!(authority
        .snapshot()
        .ready_pg_peering_completions(2_016)
        .unwrap()
        .is_empty());
    let mut fence_bridge = heartbeat_from_record(&authority, 2, peering_epoch, 2_103);
    fence_bridge.pg_observations = vec![NodePgHeartbeatObservation {
        pg_id: PgId::new(20),
        state: PgState::Peering,
        metadata_proof: active_proof,
        metadata_log_epoch: ClusterEpoch::INITIAL,
        pending_metadata_command: None,
    }];
    authority.heartbeat(fence_bridge, 2_103).unwrap();
    let mut stale_node_two_peering = heartbeat_from_record(&authority, 2, peering_epoch, 3_103);
    stale_node_two_peering.pg_observations = vec![NodePgHeartbeatObservation {
        pg_id: PgId::new(20),
        state: PgState::Peering,
        metadata_proof: PgMetadataProof::empty(),
        metadata_log_epoch: ClusterEpoch::INITIAL,
        pending_metadata_command: None,
    }];
    authority.heartbeat(stale_node_two_peering, 3_103).unwrap();
    assert!(matches!(
        authority.complete_pg_peering(
            PgId::new(20),
            NodeId::new(2),
            node_incarnation(&authority, 2),
            3_103,
        ),
        Err(ControlPlaneError::PgPeeringMetadataProofBelowFloor {
            pg_id: 20,
            node_id: 2,
            expected,
            actual,
            ..
        }) if expected == active_proof && actual == PgMetadataProof::empty()
    ));

    let mut node_two_peering = heartbeat_from_record(&authority, 2, peering_epoch, 3_104);
    node_two_peering.pg_observations = vec![NodePgHeartbeatObservation {
        pg_id: PgId::new(20),
        state: PgState::Peering,
        metadata_proof: active_proof,
        metadata_log_epoch: ClusterEpoch::INITIAL,
        pending_metadata_command: None,
    }];
    authority.heartbeat(node_two_peering, 3_104).unwrap();
    authority
        .complete_pg_peering(
            PgId::new(20),
            NodeId::new(2),
            node_incarnation(&authority, 2),
            3_104,
        )
        .unwrap();
    let mut new_active_heartbeat =
        heartbeat_from_record(&authority, 2, authority.snapshot().cluster_epoch(), 3_105);
    new_active_heartbeat.pg_observations = vec![NodePgHeartbeatObservation {
        pg_id: PgId::new(20),
        state: PgState::Active,
        metadata_proof: active_proof,
        metadata_log_epoch: ClusterEpoch::INITIAL,
        pending_metadata_command: None,
    }];
    let new_active = authority.heartbeat(new_active_heartbeat, 3_105).unwrap();
    let new_epoch = new_active.cluster_epoch();
    assert!(new_epoch > peering_epoch);

    authority
        .heartbeat(
            heartbeat_from_record(&authority, 1, new_epoch, 3_106),
            3_106,
        )
        .unwrap();
    assert!(matches!(
        authority.authorize_pg_operation(
            PgServiceOperation::MetadataWrite,
            PgId::new(20),
            NodeId::new(1),
            node_incarnation(&authority, 1),
            new_epoch,
            3_107,
        ),
        Err(ControlPlaneError::NodeNotPgPrimary {
            pg_id: 20,
            node_id: 1,
            primary_node_id: 2,
            ..
        })
    ));

    let new_authorization = authority
        .authorize_pg_operation(
            PgServiceOperation::MetadataWrite,
            PgId::new(20),
            NodeId::new(2),
            node_incarnation(&authority, 2),
            new_epoch,
            3_108,
        )
        .unwrap();
    assert_eq!(new_authorization.primary_node_id(), NodeId::new(2));
    authority
        .validate_pg_operation_authorization(&new_authorization, 2_109)
        .unwrap();
}

#[test]
fn active_metadata_pg_acting_set_change_requires_authoritative_overlap() {
    let tmp = test_util::tempdir();
    let store = FileControlPlaneStore::new(tmp.path().join("control-plane.state"));
    let mut authority = SingleAuthorityControlPlane::open(store).unwrap();
    for node_id in [1, 2] {
        authority
            .set_node_membership(NodeId::new(node_id), NodeMembershipState::Active)
            .unwrap();
        assert!(heartbeat_until_serving(&mut authority, node_id, 1_000).serving());
    }
    let active_proof = PgMetadataProof::current(9, 10, 11);
    authority
        .set_pg_acting_set(PgId::new(40), vec![NodeId::new(1)])
        .unwrap();
    heartbeat_with_pg_proof(
        &mut authority,
        1,
        40,
        PgState::Peering,
        active_proof,
        false,
        2_000,
    );
    authority
        .complete_pg_peering(
            PgId::new(40),
            NodeId::new(1),
            node_incarnation(&authority, 1),
            2_001,
        )
        .unwrap();
    heartbeat_with_pg_proof(
        &mut authority,
        1,
        40,
        PgState::Active,
        active_proof,
        false,
        2_002,
    );

    assert!(matches!(
        authority.set_pg_acting_set(PgId::new(40), vec![NodeId::new(2)]),
        Err(ControlPlaneError::PgMetadataMigrationRequiresTransfer { pg_id: 40 })
    ));
    let pg = authority.snapshot().pg(PgId::new(40)).unwrap();
    assert_eq!(pg.state(), PgState::Active);
    assert_eq!(pg.acting_set(), &[NodeId::new(1)]);
    assert_eq!(pg.active_metadata_proof(), Some(active_proof));
}

#[test]
fn active_metadata_migration_waits_for_source_after_unrelated_epoch_change() {
    let tmp = test_util::tempdir();
    let store = FileControlPlaneStore::new(tmp.path().join("control-plane.state"));
    let mut authority = SingleAuthorityControlPlane::open(store).unwrap();
    for node_id in [1, 2] {
        authority
            .set_node_membership(NodeId::new(node_id), NodeMembershipState::Active)
            .unwrap();
        assert!(heartbeat_until_serving(&mut authority, node_id, 1_000).serving());
    }

    let target_pg_id = PgId::new(40);
    let unrelated_pg_id = PgId::new(41);
    let active_proof = PgMetadataProof::current(9, 10, 11);
    authority
        .set_pg_acting_set(target_pg_id, vec![NodeId::new(1)])
        .unwrap();
    heartbeat_with_pg_proof(
        &mut authority,
        1,
        target_pg_id.get(),
        PgState::Peering,
        active_proof,
        false,
        2_000,
    );
    authority
        .complete_pg_peering(
            target_pg_id,
            NodeId::new(1),
            node_incarnation(&authority, 1),
            2_001,
        )
        .unwrap();
    heartbeat_with_pg_proof(
        &mut authority,
        1,
        target_pg_id.get(),
        PgState::Active,
        active_proof,
        false,
        2_002,
    );
    authority
        .set_pg_acting_set(unrelated_pg_id, vec![NodeId::new(1)])
        .unwrap();
    let current_epoch = authority.snapshot().cluster_epoch();
    assert!(authority
        .snapshot()
        .node(NodeId::new(1))
        .unwrap()
        .pg_observation(target_pg_id)
        .is_none());
    let before = authority.snapshot().clone();
    assert!(matches!(
        authority.set_pg_acting_set(target_pg_id, vec![NodeId::new(1), NodeId::new(2)]),
        Err(ControlPlaneError::PgMetadataMigrationSourceNotReady {
            pg_id: 40,
            cluster_epoch,
        }) if cluster_epoch == current_epoch
    ));
    assert_eq!(authority.snapshot(), &before);

    heartbeat_with_pg_proof(
        &mut authority,
        1,
        target_pg_id.get(),
        PgState::Active,
        active_proof,
        false,
        2_003,
    );

    authority
        .set_pg_acting_set(target_pg_id, vec![NodeId::new(1), NodeId::new(2)])
        .unwrap();
    let target = authority.snapshot().pg(target_pg_id).unwrap();
    assert_eq!(target.state(), PgState::Peering);
    assert_eq!(target.acting_set(), &[NodeId::new(1), NodeId::new(2)]);
    assert_eq!(target.peering_metadata_proof_floor(), Some(active_proof));
}

#[test]
fn active_metadata_overlap_migration_does_not_relax_non_primary_imported_proof() {
    let tmp = test_util::tempdir();
    let store = FileControlPlaneStore::new(tmp.path().join("control-plane.state"));
    let mut authority = SingleAuthorityControlPlane::open(store).unwrap();
    for node_id in [1, 2, 3] {
        authority
            .set_node_membership(NodeId::new(node_id), NodeMembershipState::Active)
            .unwrap();
        assert!(heartbeat_until_serving(&mut authority, node_id, 1_000).serving());
    }
    let imported_floor = PgMetadataProof::current(9, 10, 11);
    authority
        .set_pg_acting_set(PgId::new(45), vec![NodeId::new(1), NodeId::new(2)])
        .unwrap();
    for node_id in [1, 2] {
        heartbeat_with_pg_proof(
            &mut authority,
            node_id,
            45,
            PgState::Peering,
            imported_floor,
            false,
            2_000 + u64::from(node_id),
        );
    }
    authority
        .complete_pg_peering(
            PgId::new(45),
            NodeId::new(1),
            node_incarnation(&authority, 1),
            2_010,
        )
        .unwrap();
    {
        let pg = authority.snapshot.pgs.get_mut(&PgId::new(45)).unwrap();
        pg.active_metadata_transfer_imported = true;
    }
    persist_manually_modified_test_snapshot(&mut authority);
    heartbeat_with_pg_proof(
        &mut authority,
        1,
        45,
        PgState::Active,
        imported_floor,
        false,
        2_020,
    );
    authority
        .set_pg_acting_set(PgId::new(47), vec![NodeId::new(1)])
        .unwrap();

    let epoch_local_progress = PgMetadataProof::current(
        imported_floor.applied_log_index,
        imported_floor.applied_log_hash + 1,
        imported_floor.state_digest + 1,
    );
    heartbeat_with_pg_proof(
        &mut authority,
        2,
        45,
        PgState::Active,
        epoch_local_progress,
        false,
        2_021,
    );
    assert!(matches!(
        authority.set_pg_acting_set(PgId::new(45), vec![NodeId::new(2), NodeId::new(3)]),
        Err(ControlPlaneError::PgMetadataMigrationRequiresTransfer { pg_id: 45 })
    ));
    let pg = authority.snapshot().pg(PgId::new(45)).unwrap();
    assert_eq!(pg.state(), PgState::Active);
    assert_eq!(pg.acting_set(), &[NodeId::new(1), NodeId::new(2)]);

    let pg_log_epoch = authority.snapshot().cluster_epoch();
    heartbeat_with_pg_proof_at_log_epoch(
        &mut authority,
        1,
        45,
        epoch_local_progress,
        pg_log_epoch,
        2_022,
    );
    {
        let pg = authority.snapshot().pg(PgId::new(45)).unwrap();
        assert_eq!(pg.active_metadata_proof(), Some(epoch_local_progress));
        assert!(!pg.active_metadata_transfer_imported);
    }
    authority
        .set_pg_acting_set(PgId::new(45), vec![NodeId::new(1), NodeId::new(3)])
        .unwrap();
    let pg = authority.snapshot().pg(PgId::new(45)).unwrap();
    assert_eq!(pg.state(), PgState::Peering);
    assert_eq!(pg.acting_set(), &[NodeId::new(1), NodeId::new(3)]);
    assert_eq!(
        pg.peering_metadata_proof_floor(),
        Some(epoch_local_progress)
    );

    let imported_high_index_floor = PgMetadataProof::current(20, 30, 40);
    let primary_destination_progress = PgMetadataProof::current(2, 31, 41);
    authority
        .set_pg_acting_set(PgId::new(46), vec![NodeId::new(1), NodeId::new(2)])
        .unwrap();
    for node_id in [1, 2] {
        heartbeat_with_pg_proof(
            &mut authority,
            node_id,
            46,
            PgState::Peering,
            imported_high_index_floor,
            false,
            3_000 + u64::from(node_id),
        );
    }
    authority
        .complete_pg_peering(
            PgId::new(46),
            NodeId::new(1),
            node_incarnation(&authority, 1),
            3_010,
        )
        .unwrap();
    {
        let pg = authority.snapshot.pgs.get_mut(&PgId::new(46)).unwrap();
        pg.active_metadata_transfer_imported = true;
    }
    persist_manually_modified_test_snapshot(&mut authority);
    heartbeat_with_pg_proof(
        &mut authority,
        2,
        46,
        PgState::Active,
        imported_high_index_floor,
        false,
        3_020,
    );
    authority
        .set_pg_acting_set(PgId::new(48), vec![NodeId::new(1)])
        .unwrap();
    let pg_log_epoch = authority.snapshot().cluster_epoch();
    heartbeat_with_pg_proof_at_log_epoch(
        &mut authority,
        1,
        46,
        primary_destination_progress,
        pg_log_epoch,
        3_021,
    );
    {
        let pg = authority.snapshot().pg(PgId::new(46)).unwrap();
        assert_eq!(
            pg.active_metadata_proof(),
            Some(primary_destination_progress)
        );
        assert!(!pg.active_metadata_transfer_imported);
    }
    authority
        .set_pg_acting_set(
            PgId::new(46),
            vec![NodeId::new(1), NodeId::new(2), NodeId::new(3)],
        )
        .unwrap();
    let pg = authority.snapshot().pg(PgId::new(46)).unwrap();
    assert_eq!(pg.state(), PgState::Peering);
    assert_eq!(
        pg.peering_metadata_proof_floor(),
        Some(primary_destination_progress)
    );
}

#[test]
fn imported_active_primary_restart_preserves_epoch_local_peering_floor() {
    let tmp = test_util::tempdir();
    let store = FileControlPlaneStore::new(tmp.path().join("control-plane.state"));
    let mut authority = SingleAuthorityControlPlane::open(store).unwrap();
    authority
        .set_node_membership(NodeId::new(1), NodeMembershipState::Active)
        .unwrap();
    assert!(heartbeat_until_serving(&mut authority, 1, 1_000).serving());

    let imported_proof = PgMetadataProof::current(42, 100, 200);
    authority
        .set_pg_acting_set(PgId::new(49), vec![NodeId::new(1)])
        .unwrap();
    heartbeat_with_pg_proof(
        &mut authority,
        1,
        49,
        PgState::Peering,
        imported_proof,
        false,
        2_000,
    );
    authority
        .complete_pg_peering(
            PgId::new(49),
            NodeId::new(1),
            node_incarnation(&authority, 1),
            2_001,
        )
        .unwrap();
    {
        let pg = authority.snapshot.pgs.get_mut(&PgId::new(49)).unwrap();
        pg.active_metadata_transfer_imported = true;
    }
    persist_manually_modified_test_snapshot(&mut authority);

    let epoch_local_proof = PgMetadataProof::current(
        imported_proof.applied_log_index,
        imported_proof.applied_log_hash + 1,
        imported_proof.state_digest + 1,
    );
    let active_epoch = authority.snapshot().cluster_epoch();
    let mut restarting_primary = heartbeat_from_record(&authority, 1, active_epoch, 2_002);
    restarting_primary.node_incarnation += 1;
    restarting_primary.pg_observations = vec![NodePgHeartbeatObservation {
        pg_id: PgId::new(49),
        state: PgState::Peering,
        metadata_proof: epoch_local_proof,
        metadata_log_epoch: active_epoch,
        pending_metadata_command: None,
    }];
    authority.heartbeat(restarting_primary, 2_002).unwrap();
    let peering_epoch = authority.snapshot().cluster_epoch();
    let pg = authority.snapshot().pg(PgId::new(49)).unwrap();
    assert_eq!(pg.state(), PgState::Peering);
    assert_eq!(pg.peering_metadata_proof_floor(), Some(epoch_local_proof));

    let mut current_peering = heartbeat_from_record(&authority, 1, peering_epoch, 2_003);
    current_peering.pg_observations = vec![NodePgHeartbeatObservation {
        pg_id: PgId::new(49),
        state: PgState::Peering,
        metadata_proof: epoch_local_proof,
        metadata_log_epoch: active_epoch,
        pending_metadata_command: None,
    }];
    authority.heartbeat(current_peering, 2_003).unwrap();
    assert!(matches!(
        authority.complete_pg_peering(
            PgId::new(49),
            NodeId::new(1),
            node_incarnation(&authority, 1),
            2_004,
        ),
        Err(ControlPlaneError::PgPreviousPrimaryLeaseStillActive { pg_id: 49, .. })
    ));
    heartbeat_with_pg_proof(
        &mut authority,
        1,
        49,
        PgState::Peering,
        epoch_local_proof,
        false,
        2_100,
    );
    heartbeat_with_pg_proof(
        &mut authority,
        1,
        49,
        PgState::Peering,
        epoch_local_proof,
        false,
        3_100,
    );
    authority
        .complete_pg_peering(
            PgId::new(49),
            NodeId::new(1),
            node_incarnation(&authority, 1),
            3_100,
        )
        .unwrap();
    let pg = authority.snapshot().pg(PgId::new(49)).unwrap();
    assert_eq!(pg.state(), PgState::Active);
    assert_eq!(pg.active_metadata_proof(), Some(epoch_local_proof));
    assert!(!pg.active_metadata_transfer_imported());
}

#[test]
fn imported_active_restart_without_initial_observation_accepts_later_epoch_local_peering_proof() {
    let tmp = test_util::tempdir();
    let store = FileControlPlaneStore::new(tmp.path().join("control-plane.state"));
    let mut authority = SingleAuthorityControlPlane::open(store).unwrap();
    authority
        .set_node_membership(NodeId::new(1), NodeMembershipState::Active)
        .unwrap();
    assert!(heartbeat_until_serving(&mut authority, 1, 1_000).serving());

    let imported_proof = PgMetadataProof::current(42, 100, 200);
    authority
        .set_pg_acting_set(PgId::new(50), vec![NodeId::new(1)])
        .unwrap();
    heartbeat_with_pg_proof(
        &mut authority,
        1,
        50,
        PgState::Peering,
        imported_proof,
        false,
        2_000,
    );
    authority
        .complete_pg_peering(
            PgId::new(50),
            NodeId::new(1),
            node_incarnation(&authority, 1),
            2_001,
        )
        .unwrap();
    {
        let pg = authority.snapshot.pgs.get_mut(&PgId::new(50)).unwrap();
        pg.active_metadata_transfer_imported = true;
    }
    persist_manually_modified_test_snapshot(&mut authority);

    let epoch_local_proof = PgMetadataProof::current(
        imported_proof.applied_log_index,
        imported_proof.applied_log_hash + 1,
        imported_proof.state_digest + 1,
    );
    let active_proof_epoch = authority
        .snapshot()
        .pg(PgId::new(50))
        .unwrap()
        .active_metadata_proof_epoch()
        .unwrap();
    let active_route_epoch = authority.snapshot().cluster_epoch();
    let mut restarting_primary = heartbeat_from_record(&authority, 1, active_route_epoch, 2_002);
    restarting_primary.node_incarnation += 1;
    authority.heartbeat(restarting_primary, 2_002).unwrap();
    let peering_epoch = authority.snapshot().cluster_epoch();
    let pg = authority.snapshot().pg(PgId::new(50)).unwrap();
    assert_eq!(pg.state(), PgState::Peering);
    assert_eq!(pg.peering_metadata_proof_floor(), Some(imported_proof));
    assert_eq!(
        pg.peering_metadata_proof_floor_epoch(),
        Some(active_proof_epoch)
    );
    assert!(pg.peering_metadata_proof_floor_imported());

    let mut current_peering = heartbeat_from_record(&authority, 1, peering_epoch, 2_003);
    current_peering.pg_observations = vec![NodePgHeartbeatObservation {
        pg_id: PgId::new(50),
        state: PgState::Peering,
        metadata_proof: epoch_local_proof,
        metadata_log_epoch: ClusterEpoch::INITIAL,
        pending_metadata_command: None,
    }];
    authority.heartbeat(current_peering, 2_003).unwrap();
    assert!(matches!(
        authority.complete_pg_peering(
            PgId::new(50),
            NodeId::new(1),
            node_incarnation(&authority, 1),
            2_004,
        ),
        Err(ControlPlaneError::PgPreviousPrimaryLeaseStillActive { pg_id: 50, .. })
    ));
    heartbeat_with_pg_proof(
        &mut authority,
        1,
        50,
        PgState::Peering,
        epoch_local_proof,
        false,
        2_100,
    );
    heartbeat_with_pg_proof(
        &mut authority,
        1,
        50,
        PgState::Peering,
        epoch_local_proof,
        false,
        3_100,
    );
    authority
        .complete_pg_peering(
            PgId::new(50),
            NodeId::new(1),
            node_incarnation(&authority, 1),
            3_100,
        )
        .unwrap();
    let pg = authority.snapshot().pg(PgId::new(50)).unwrap();
    assert_eq!(pg.state(), PgState::Active);
    assert_eq!(pg.active_metadata_proof(), Some(epoch_local_proof));
    assert!(!pg.active_metadata_transfer_imported());
}

#[test]
fn peering_metadata_pg_acting_set_change_preserves_floor_and_requires_source() {
    let tmp = test_util::tempdir();
    let store = FileControlPlaneStore::new(tmp.path().join("control-plane.state"));
    let mut authority = SingleAuthorityControlPlane::open(store).unwrap();
    for node_id in [1, 2, 3] {
        authority
            .set_node_membership(NodeId::new(node_id), NodeMembershipState::Active)
            .unwrap();
        assert!(heartbeat_until_serving(&mut authority, node_id, 1_000).serving());
    }
    let active_proof = PgMetadataProof::current(9, 10, 11);
    authority
        .set_pg_acting_set(PgId::new(41), vec![NodeId::new(1)])
        .unwrap();
    heartbeat_with_pg_proof(
        &mut authority,
        1,
        41,
        PgState::Peering,
        active_proof,
        false,
        2_000,
    );
    authority
        .complete_pg_peering(
            PgId::new(41),
            NodeId::new(1),
            node_incarnation(&authority, 1),
            2_001,
        )
        .unwrap();
    heartbeat_with_pg_proof(
        &mut authority,
        1,
        41,
        PgState::Active,
        active_proof,
        false,
        2_002,
    );
    authority
        .set_pg_acting_set(PgId::new(41), vec![NodeId::new(1), NodeId::new(2)])
        .unwrap();
    let peering_epoch = authority.snapshot().cluster_epoch();
    let pg = authority.snapshot().pg(PgId::new(41)).unwrap();
    assert_eq!(pg.state(), PgState::Peering);
    assert_eq!(pg.peering_metadata_proof_floor(), Some(active_proof));

    assert!(matches!(
        authority.set_pg_acting_set(PgId::new(41), vec![NodeId::new(2)]),
        Err(ControlPlaneError::PgMetadataMigrationRequiresTransfer { pg_id: 41 })
    ));
    let pg = authority.snapshot().pg(PgId::new(41)).unwrap();
    assert_eq!(authority.snapshot().cluster_epoch(), peering_epoch);
    assert_eq!(pg.state(), PgState::Peering);
    assert_eq!(pg.acting_set(), &[NodeId::new(1), NodeId::new(2)]);
    assert_eq!(pg.peering_metadata_proof_floor(), Some(active_proof));

    heartbeat_with_pg_proof(
        &mut authority,
        1,
        41,
        PgState::Peering,
        active_proof,
        false,
        2_003,
    );
    authority
        .set_pg_acting_set(PgId::new(41), vec![NodeId::new(1), NodeId::new(3)])
        .unwrap();
    let pg = authority.snapshot().pg(PgId::new(41)).unwrap();
    assert_eq!(pg.state(), PgState::Peering);
    assert_eq!(pg.acting_set(), &[NodeId::new(1), NodeId::new(3)]);
    assert_eq!(pg.peering_metadata_proof_floor(), Some(active_proof));
}

#[test]
fn pg_transition_graph_survives_file_reopen_and_continues() {
    let tmp = test_util::tempdir();
    let store = FileControlPlaneStore::new(tmp.path().join("control-plane.state"));
    let mut authority = SingleAuthorityControlPlane::open(store.clone()).unwrap();
    for node_id in [1, 2, 3] {
        authority
            .set_node_membership(NodeId::new(node_id), NodeMembershipState::Active)
            .unwrap();
        assert!(heartbeat_until_serving(&mut authority, node_id, 1_000).serving());
    }

    let pg_id = PgId::new(61);
    let initial_proof = PgMetadataProof::current(9, 10, 11);
    authority
        .set_pg_acting_set(pg_id, vec![NodeId::new(1)])
        .unwrap();
    assert_persisted_snapshot_matches_authority(&authority, &store);
    authority = reopen_file_authority(&store);
    assert_eq!(
        authority.snapshot().pg(pg_id).unwrap().state(),
        PgState::Peering
    );

    heartbeat_with_pg_proof_and_lease_duration(
        &mut authority,
        1,
        pg_id.get(),
        PgState::Peering,
        initial_proof,
        false,
        (2_000, 5),
    );
    authority
        .complete_pg_peering(
            pg_id,
            NodeId::new(1),
            node_incarnation(&authority, 1),
            2_001,
        )
        .unwrap();
    assert_eq!(
        authority.snapshot().pg(pg_id).unwrap().state(),
        PgState::Active
    );
    assert_persisted_snapshot_matches_authority(&authority, &store);

    authority = reopen_file_authority(&store);
    let restarted_pg = authority.snapshot().pg(pg_id).unwrap();
    assert_eq!(restarted_pg.state(), PgState::Peering);
    assert_eq!(
        restarted_pg.peering_metadata_proof_floor(),
        Some(initial_proof)
    );
    assert_eq!(restarted_pg.peering_metadata_transfer(), None);

    heartbeat_with_pg_proof_and_lease_duration(
        &mut authority,
        1,
        pg_id.get(),
        PgState::Peering,
        initial_proof,
        false,
        (2_010, 5),
    );
    authority
        .complete_pg_peering(
            pg_id,
            NodeId::new(1),
            node_incarnation(&authority, 1),
            2_011,
        )
        .unwrap();
    heartbeat_with_pg_proof_and_lease_duration(
        &mut authority,
        1,
        pg_id.get(),
        PgState::Active,
        initial_proof,
        false,
        (2_012, 5),
    );

    let expected_overlap_floor_epoch = authority
        .snapshot()
        .pg(pg_id)
        .unwrap()
        .active_metadata_proof_epoch();
    authority
        .set_pg_acting_set(pg_id, vec![NodeId::new(1), NodeId::new(2)])
        .unwrap();
    let overlap_pg = authority.snapshot().pg(pg_id).unwrap();
    assert_eq!(overlap_pg.state(), PgState::Peering);
    assert_eq!(
        overlap_pg.peering_metadata_proof_floor(),
        Some(initial_proof)
    );
    assert_persisted_snapshot_matches_authority(&authority, &store);

    authority = reopen_file_authority(&store);
    let overlap_pg = authority.snapshot().pg(pg_id).unwrap();
    assert_eq!(overlap_pg.state(), PgState::Peering);
    assert_eq!(
        overlap_pg.peering_metadata_proof_floor(),
        Some(initial_proof)
    );
    assert_eq!(
        overlap_pg.peering_metadata_proof_floor_epoch(),
        expected_overlap_floor_epoch
    );
    for (node_id, now_ms) in [(1, 2_020), (2, 2_021)] {
        heartbeat_with_pg_proof_and_lease_duration(
            &mut authority,
            node_id,
            pg_id.get(),
            PgState::Peering,
            initial_proof,
            false,
            (now_ms, 5),
        );
    }
    assert_eq!(
        authority.complete_ready_pg_peerings(2_022).unwrap(),
        vec![pg_id]
    );
    let active_pg = authority.snapshot().pg(pg_id).unwrap();
    assert_eq!(active_pg.state(), PgState::Active);
    assert_eq!(active_pg.active_metadata_proof(), Some(initial_proof));
    assert_persisted_snapshot_matches_authority(&authority, &store);

    authority = reopen_file_authority(&store);
    for (node_id, now_ms) in [(1, 2_030), (2, 2_031)] {
        heartbeat_with_pg_proof_and_lease_duration(
            &mut authority,
            node_id,
            pg_id.get(),
            PgState::Peering,
            initial_proof,
            false,
            (now_ms, 5),
        );
    }
    authority.complete_ready_pg_peerings(2_032).unwrap();
    let active_epoch = authority.snapshot().cluster_epoch();
    let active_pg = authority.snapshot().pg(pg_id).unwrap();
    let source_primary = active_pg.active_primary().unwrap();
    let source_proof = active_pg.active_metadata_proof().unwrap();
    let imported_proof = PgMetadataProof::current(
        source_proof.applied_log_index + 1,
        source_proof.applied_log_hash + 100,
        source_proof.state_digest + 100,
    );
    let transfer = PgMetadataTransferProof::new_with_imported_metadata_proof(
        active_epoch,
        source_proof,
        imported_proof,
    );
    authority
        .set_pg_acting_set_with_metadata_transfer(pg_id, vec![NodeId::new(3)], transfer)
        .unwrap();
    let transfer_pg = authority.snapshot().pg(pg_id).unwrap();
    assert_eq!(transfer_pg.state(), PgState::Peering);
    assert_eq!(transfer_pg.acting_set(), &[NodeId::new(3)]);
    assert_eq!(transfer_pg.peering_metadata_transfer(), Some(transfer));
    assert_eq!(
        transfer_pg.peering_metadata_transfer_source_route_epoch(),
        Some(active_epoch)
    );
    assert_eq!(
        transfer_pg.peering_metadata_transfer_source_node_id(),
        Some(source_primary)
    );
    assert_persisted_snapshot_matches_authority(&authority, &store);

    authority = reopen_file_authority(&store);
    let transfer_pg = authority.snapshot().pg(pg_id).unwrap();
    assert_eq!(transfer_pg.state(), PgState::Peering);
    assert_eq!(transfer_pg.peering_metadata_transfer(), Some(transfer));
    assert_eq!(
        transfer_pg.peering_metadata_proof_floor(),
        Some(imported_proof)
    );
    assert_eq!(
        transfer_pg.peering_metadata_proof_floor_epoch(),
        Some(active_epoch)
    );
    heartbeat_with_pg_proof_and_lease_duration(
        &mut authority,
        3,
        pg_id.get(),
        PgState::Peering,
        imported_proof,
        false,
        (2_040, 5),
    );
    heartbeat_with_pg_proof_and_lease_duration(
        &mut authority,
        3,
        pg_id.get(),
        PgState::Peering,
        imported_proof,
        false,
        (3_035, 5),
    );
    authority
        .complete_pg_peering(
            pg_id,
            NodeId::new(3),
            node_incarnation(&authority, 3),
            3_035,
        )
        .unwrap();
    let imported_active_pg = authority.snapshot().pg(pg_id).unwrap();
    assert_eq!(imported_active_pg.state(), PgState::Active);
    assert_eq!(
        imported_active_pg.active_metadata_proof(),
        Some(imported_proof)
    );
    assert!(imported_active_pg.active_metadata_transfer_imported());
    assert_persisted_snapshot_matches_authority(&authority, &store);

    authority = reopen_file_authority(&store);
    let restarted_import_pg = authority.snapshot().pg(pg_id).unwrap();
    assert_eq!(restarted_import_pg.state(), PgState::Peering);
    assert_eq!(
        restarted_import_pg.peering_metadata_proof_floor(),
        Some(imported_proof)
    );
    assert!(restarted_import_pg.peering_metadata_proof_floor_imported());
    assert_eq!(restarted_import_pg.peering_metadata_transfer(), None);
    heartbeat_with_pg_proof(
        &mut authority,
        3,
        pg_id.get(),
        PgState::Peering,
        imported_proof,
        false,
        3_050,
    );
    authority
        .complete_pg_peering(
            pg_id,
            NodeId::new(3),
            node_incarnation(&authority, 3),
            3_051,
        )
        .unwrap();
    assert_eq!(
        authority.snapshot().pg(pg_id).unwrap().state(),
        PgState::Active
    );
}

#[test]
fn metadata_transfer_allows_explicit_non_overlap_pg_migration() {
    let tmp = test_util::tempdir();
    let store = FileControlPlaneStore::new(tmp.path().join("control-plane.state"));
    let mut authority = SingleAuthorityControlPlane::open(store.clone()).unwrap();
    for node_id in [1, 2] {
        authority
            .set_node_membership(NodeId::new(node_id), NodeMembershipState::Active)
            .unwrap();
        assert!(heartbeat_until_serving(&mut authority, node_id, 1_000).serving());
    }
    let active_proof = PgMetadataProof::current(9, 10, 11);
    authority
        .set_pg_acting_set(PgId::new(42), vec![NodeId::new(1)])
        .unwrap();
    heartbeat_with_pg_proof_and_lease_duration(
        &mut authority,
        1,
        42,
        PgState::Peering,
        active_proof,
        false,
        (2_000, 3),
    );
    authority
        .complete_pg_peering(
            PgId::new(42),
            NodeId::new(1),
            node_incarnation(&authority, 1),
            2_001,
        )
        .unwrap();
    heartbeat_with_pg_proof_and_lease_duration(
        &mut authority,
        1,
        42,
        PgState::Active,
        active_proof,
        false,
        (2_002, 1),
    );
    let active_epoch = authority.snapshot().cluster_epoch();

    assert!(matches!(
        authority.set_pg_acting_set(PgId::new(42), vec![NodeId::new(2)]),
        Err(ControlPlaneError::PgMetadataMigrationRequiresTransfer { pg_id: 42 })
    ));

    let stale_transfer = PgMetadataTransferProof::new_with_imported_metadata_proof(
        active_epoch,
        PgMetadataProof::current(
            active_proof.applied_log_index,
            active_proof.applied_log_hash + 1,
            active_proof.state_digest,
        ),
        PgMetadataProof::current(
            active_proof.applied_log_index,
            active_proof.applied_log_hash + 10,
            active_proof.state_digest,
        ),
    );
    assert!(matches!(
        authority.set_pg_acting_set_with_metadata_transfer(
            PgId::new(42),
            vec![NodeId::new(2)],
            stale_transfer,
        ),
        Err(ControlPlaneError::PgMetadataTransferProofBelowFloor { pg_id: 42, .. })
    ));

    let future_epoch = ClusterEpoch::new(active_epoch.get() + 1).unwrap();
    let future_transfer = PgMetadataTransferProof::new(future_epoch, active_proof);
    assert!(matches!(
        authority.set_pg_acting_set_with_metadata_transfer(
            PgId::new(42),
            vec![NodeId::new(2)],
            future_transfer,
        ),
        Err(ControlPlaneError::PgMetadataTransferSourceEpochInFuture { pg_id: 42, .. })
    ));

    let stale_epoch = ClusterEpoch::new(active_epoch.get() - 1).unwrap();
    let stale_epoch_transfer = PgMetadataTransferProof::new(stale_epoch, active_proof);
    assert!(matches!(
        authority.set_pg_acting_set_with_metadata_transfer(
            PgId::new(42),
            vec![NodeId::new(2)],
            stale_epoch_transfer,
        ),
        Err(ControlPlaneError::PgMetadataTransferSourceEpochStale { pg_id: 42, .. })
    ));

    let imported_proof = PgMetadataProof::current(
        active_proof.applied_log_index,
        active_proof.applied_log_hash + 100,
        active_proof.state_digest,
    );
    let transfer = PgMetadataTransferProof::new_with_imported_metadata_proof(
        active_epoch,
        active_proof,
        imported_proof,
    );
    let snapshot_without_transfer = authority.snapshot().clone();
    assert!(matches!(
        authority.set_pg_acting_set_with_metadata_transfer_at_epoch(
            PgId::new(42),
            vec![NodeId::new(1)],
            transfer,
            next_epoch(active_epoch).unwrap(),
        ),
        Err(ControlPlaneError::PgMetadataMigrationRequiresTransfer { pg_id: 42 })
    ));
    assert_eq!(authority.snapshot(), &snapshot_without_transfer);
    authority
        .set_pg_acting_set_with_metadata_transfer(PgId::new(42), vec![NodeId::new(2)], transfer)
        .unwrap();
    let peering_epoch = authority.snapshot().cluster_epoch();
    let pg = authority.snapshot().pg(PgId::new(42)).unwrap();
    assert_eq!(pg.state(), PgState::Peering);
    assert_eq!(pg.acting_set(), &[NodeId::new(2)]);
    assert_eq!(pg.peering_metadata_proof_floor(), Some(imported_proof));
    assert_eq!(pg.peering_metadata_transfer(), Some(transfer));
    assert_eq!(
        pg.peering_metadata_transfer_source_route_epoch(),
        Some(active_epoch)
    );
    assert_eq!(
        pg.peering_metadata_transfer_source_node_id(),
        Some(NodeId::new(1))
    );
    assert!(!pg.metadata_transfer_fenced());
    let mismatched_same_acting_set = PgMetadataTransferProof::new_with_imported_metadata_proof(
        active_epoch,
        active_proof,
        PgMetadataProof::current(
            imported_proof.applied_log_index,
            imported_proof.applied_log_hash + 1,
            imported_proof.state_digest,
        ),
    );
    assert!(matches!(
        authority.set_pg_acting_set_with_metadata_transfer(
            PgId::new(42),
            vec![NodeId::new(2)],
            mismatched_same_acting_set,
        ),
        Err(ControlPlaneError::PgMetadataTransferProofMismatch { pg_id: 42, .. })
    ));
    assert_eq!(authority.snapshot().cluster_epoch(), peering_epoch);
    let mismatched_destination_epoch = next_epoch(peering_epoch).unwrap();
    assert!(matches!(
        authority.set_pg_acting_set_with_metadata_transfer_at_epoch(
            PgId::new(42),
            vec![NodeId::new(2)],
            transfer,
            mismatched_destination_epoch,
        ),
        Err(ControlPlaneError::PgMetadataTransferDestinationEpochMismatch {
            pg_id: 42,
            expected_destination_epoch,
            actual_destination_epoch,
        }) if expected_destination_epoch == mismatched_destination_epoch
            && actual_destination_epoch == peering_epoch
    ));
    assert_eq!(authority.snapshot().cluster_epoch(), peering_epoch);
    authority
        .set_pg_acting_set_with_metadata_transfer_at_epoch(
            PgId::new(42),
            vec![NodeId::new(2)],
            transfer,
            peering_epoch,
        )
        .unwrap();
    assert_eq!(authority.snapshot().cluster_epoch(), peering_epoch);

    let transfer_epoch = authority.snapshot().cluster_epoch();
    assert_eq!(
        authority
            .fence_pg_for_metadata_transfer(PgId::new(42))
            .unwrap()
            .cluster_epoch(),
        transfer_epoch
    );
    let pg = authority.snapshot().pg(PgId::new(42)).unwrap();
    assert_eq!(pg.peering_metadata_transfer(), Some(transfer));
    assert!(!pg.metadata_transfer_fenced());

    let restarted = open_independent_file_store_restart(
        &store,
        tmp.path().join("control-plane-first-restart.state"),
    );
    let restarted_pg = restarted.snapshot().pg(PgId::new(42)).unwrap();
    assert_eq!(
        restarted_pg.peering_metadata_proof_floor(),
        Some(imported_proof)
    );
    assert_eq!(restarted_pg.peering_metadata_transfer(), Some(transfer));

    heartbeat_with_pg_proof(
        &mut authority,
        2,
        42,
        PgState::Peering,
        active_proof,
        false,
        2_003,
    );
    heartbeat_with_pg_proof(
        &mut authority,
        2,
        42,
        PgState::Peering,
        active_proof,
        false,
        3_003,
    );
    assert!(matches!(
        authority.complete_pg_peering(
            PgId::new(42),
            NodeId::new(2),
            node_incarnation(&authority, 2),
            3_003,
        ),
        Err(ControlPlaneError::PgPeeringMetadataProofBelowFloor {
            pg_id: 42,
            cluster_epoch,
            ..
        }) if cluster_epoch == peering_epoch
    ));

    let stale_source_above_imported = PgMetadataProof::current(
        imported_proof.applied_log_index + 10,
        imported_proof.applied_log_hash + 10,
        imported_proof.state_digest + 10,
    );
    heartbeat_with_pg_proof(
        &mut authority,
        2,
        42,
        PgState::Peering,
        stale_source_above_imported,
        false,
        3_004,
    );
    assert!(matches!(
        authority.complete_pg_peering(
            PgId::new(42),
            NodeId::new(2),
            node_incarnation(&authority, 2),
            3_004,
        ),
        Err(ControlPlaneError::PgPeeringMetadataProofBelowFloor {
            pg_id: 42,
            cluster_epoch,
            expected,
            actual,
            ..
        }) if cluster_epoch == peering_epoch
            && expected == imported_proof
            && actual == stale_source_above_imported
    ));

    heartbeat_with_pg_proof(
        &mut authority,
        2,
        42,
        PgState::Peering,
        imported_proof,
        false,
        3_005,
    );
    authority
        .complete_pg_peering(
            PgId::new(42),
            NodeId::new(2),
            node_incarnation(&authority, 2),
            3_006,
        )
        .unwrap();
    let active_pg = authority.snapshot().pg(PgId::new(42)).unwrap();
    assert_eq!(active_pg.state(), PgState::Active);
    assert_eq!(active_pg.active_primary(), Some(NodeId::new(2)));
    assert_eq!(active_pg.active_metadata_proof(), Some(imported_proof));
    assert!(active_pg.active_metadata_transfer_imported());
    assert_eq!(active_pg.peering_metadata_proof_floor(), None);
    assert_eq!(active_pg.peering_metadata_transfer(), None);

    let restarted_active = open_independent_file_store_restart(
        &store,
        tmp.path().join("control-plane-active-restart.state"),
    );
    let restarted_active_pg = restarted_active.snapshot().pg(PgId::new(42)).unwrap();
    assert_eq!(restarted_active_pg.state(), PgState::Peering);
    assert_eq!(
        restarted_active_pg.peering_metadata_proof_floor(),
        Some(imported_proof)
    );
    assert_eq!(restarted_active_pg.peering_metadata_transfer(), None);
    assert!(!restarted_active_pg.metadata_transfer_fenced());
    assert!(!restarted_active_pg.active_metadata_transfer_imported());

    let epoch_local_source_proof = PgMetadataProof::current(
        imported_proof.applied_log_index,
        imported_proof.applied_log_hash + 200,
        imported_proof.state_digest + 1,
    );
    heartbeat_with_pg_proof(
        &mut authority,
        2,
        42,
        PgState::Active,
        imported_proof,
        false,
        2_007,
    );
    let repeated_source_epoch = authority.snapshot().cluster_epoch();
    authority
        .fence_pg_for_metadata_transfer(PgId::new(42))
        .unwrap();
    let fenced_epoch = authority.snapshot().cluster_epoch();
    let fenced_pg = authority.snapshot().pg(PgId::new(42)).unwrap();
    assert_eq!(fenced_pg.state(), PgState::Peering);
    assert!(fenced_pg.metadata_transfer_fence_source_imported);
    assert_eq!(
        fenced_pg.metadata_transfer_fence_epoch(),
        Some(fenced_epoch)
    );
    assert_eq!(
        fenced_pg.peering_metadata_proof_floor(),
        Some(imported_proof)
    );
    let repeated_transfer = PgMetadataTransferProof::new_with_imported_metadata_proof(
        repeated_source_epoch,
        epoch_local_source_proof,
        PgMetadataProof::current(
            epoch_local_source_proof.applied_log_index,
            epoch_local_source_proof.applied_log_hash + 100,
            epoch_local_source_proof.state_digest,
        ),
    );
    authority
        .set_pg_acting_set_with_metadata_transfer(
            PgId::new(42),
            vec![NodeId::new(1)],
            repeated_transfer,
        )
        .unwrap();
    let repeated_transfer_pg = authority.snapshot().pg(PgId::new(42)).unwrap();
    assert_eq!(repeated_transfer_pg.acting_set(), &[NodeId::new(1)]);
    assert_eq!(
        repeated_transfer_pg.peering_metadata_transfer(),
        Some(repeated_transfer)
    );
}

#[test]
fn fenced_metadata_transfer_retry_without_stored_deadline_uses_max_source_lease() {
    let tmp = test_util::tempdir();
    let store = FileControlPlaneStore::new(tmp.path().join("control-plane.state"));
    let mut authority = SingleAuthorityControlPlane::open(store).unwrap();
    for node_id in [1, 2] {
        authority
            .set_node_membership(NodeId::new(node_id), NodeMembershipState::Active)
            .unwrap();
        assert!(heartbeat_until_serving(&mut authority, node_id, 1_000).serving());
    }
    let active_proof = PgMetadataProof::current(9, 10, 11);
    authority
        .set_pg_acting_set(PgId::new(50), vec![NodeId::new(1), NodeId::new(2)])
        .unwrap();
    heartbeat_with_pg_proof(
        &mut authority,
        1,
        50,
        PgState::Peering,
        active_proof,
        false,
        2_000,
    );
    heartbeat_with_pg_proof(
        &mut authority,
        2,
        50,
        PgState::Peering,
        active_proof,
        false,
        2_001,
    );
    authority
        .complete_pg_peering(
            PgId::new(50),
            NodeId::new(1),
            node_incarnation(&authority, 1),
            2_002,
        )
        .unwrap();
    heartbeat_with_pg_proof(
        &mut authority,
        1,
        50,
        PgState::Active,
        active_proof,
        false,
        2_003,
    );
    heartbeat_with_pg_proof(
        &mut authority,
        2,
        50,
        PgState::Active,
        active_proof,
        false,
        2_020,
    );

    authority
        .fence_pg_for_metadata_transfer_with_source_lease(PgId::new(50))
        .unwrap();
    let record = authority
        .snapshot
        .pgs
        .get_mut(&PgId::new(50))
        .expect("test PG should exist");
    assert!(record.metadata_transfer_fenced);
    record.metadata_transfer_fence_source_lease_deadline_ms = None;
    persist_manually_modified_test_snapshot(&mut authority);

    let retry = authority
        .fence_pg_for_metadata_transfer_with_source_lease(PgId::new(50))
        .unwrap();

    assert_eq!(retry.source_primary_lease_deadline_ms(), Some(2_120));
}

#[test]
fn fencing_pending_recovery_peering_preserves_imported_source_provenance() {
    let tmp = test_util::tempdir();
    let store = FileControlPlaneStore::new(tmp.path().join("control-plane.state"));
    let mut authority = SingleAuthorityControlPlane::open(store).unwrap();
    for node_id in [1, 2] {
        authority
            .set_node_membership(NodeId::new(node_id), NodeMembershipState::Active)
            .unwrap();
        assert!(heartbeat_until_serving(&mut authority, node_id, 1_000).serving());
    }

    let pg_id = PgId::new(52);
    let source_proof = PgMetadataProof::current(9, 10, 11);
    authority
        .set_pg_acting_set(pg_id, vec![NodeId::new(1)])
        .unwrap();
    heartbeat_with_pg_proof(
        &mut authority,
        1,
        pg_id.get(),
        PgState::Peering,
        source_proof,
        false,
        2_000,
    );
    authority
        .complete_pg_peering(
            pg_id,
            NodeId::new(1),
            node_incarnation(&authority, 1),
            2_001,
        )
        .unwrap();
    heartbeat_with_pg_proof(
        &mut authority,
        1,
        pg_id.get(),
        PgState::Active,
        source_proof,
        false,
        2_002,
    );

    authority.fence_pg_for_metadata_transfer(pg_id).unwrap();
    let imported_proof = PgMetadataProof::current(3, 20, 21);
    let first_transfer = PgMetadataTransferProof::new_with_imported_metadata_proof(
        authority.snapshot().cluster_epoch(),
        source_proof,
        imported_proof,
    );
    authority
        .set_pg_acting_set_with_metadata_transfer(pg_id, vec![NodeId::new(2)], first_transfer)
        .unwrap();
    heartbeat_with_pg_proof(
        &mut authority,
        2,
        pg_id.get(),
        PgState::Peering,
        imported_proof,
        false,
        2_010,
    );
    heartbeat_with_pg_proof(
        &mut authority,
        2,
        pg_id.get(),
        PgState::Peering,
        imported_proof,
        false,
        3_200,
    );
    authority
        .complete_pg_peering(
            pg_id,
            NodeId::new(2),
            node_incarnation(&authority, 2),
            3_201,
        )
        .unwrap();
    let active_epoch = authority.snapshot().cluster_epoch();
    assert!(authority
        .snapshot()
        .pg(pg_id)
        .unwrap()
        .active_metadata_transfer_imported());

    let local_progress = PgMetadataProof::current(2, 30, 31);
    let pending = test_pending_metadata_command(active_epoch);
    authority
        .set_pg_acting_set(PgId::new(54), vec![NodeId::new(1)])
        .unwrap();
    let report_epoch = authority.snapshot().cluster_epoch();
    let mut pending_heartbeat = heartbeat_from_record(&authority, 2, report_epoch, 3_220);
    pending_heartbeat.cluster_map_history_route_references =
        history_route_references([PgClusterMapHistoryRouteReference::new(
            PgClusterMapHistoryRouteReferenceKind::MetadataCommandResource,
            active_epoch,
            pg_id,
        )]);
    pending_heartbeat.pg_observations = vec![NodePgHeartbeatObservation {
        pg_id,
        state: PgState::Active,
        metadata_proof: local_progress,
        metadata_log_epoch: ClusterEpoch::INITIAL,
        pending_metadata_command: Some(pending),
    }];
    authority.heartbeat(pending_heartbeat, 3_220).unwrap();
    let recovery_epoch = authority.snapshot().cluster_epoch();
    let recovering = authority.snapshot().pg(pg_id).unwrap();
    assert_eq!(recovering.state(), PgState::Peering);
    assert_eq!(
        recovering.peering_metadata_proof_floor(),
        Some(imported_proof)
    );
    assert!(recovering.peering_metadata_proof_floor_imported());

    let mut cleared_heartbeat = heartbeat_from_record(&authority, 2, recovery_epoch, 3_221);
    cleared_heartbeat.pg_observations = vec![NodePgHeartbeatObservation {
        pg_id,
        state: PgState::Peering,
        metadata_proof: local_progress,
        metadata_log_epoch: ClusterEpoch::INITIAL,
        pending_metadata_command: None,
    }];
    authority.heartbeat(cleared_heartbeat, 3_221).unwrap();
    authority.fence_pg_for_metadata_transfer(pg_id).unwrap();
    let fenced = authority.snapshot().pg(pg_id).unwrap();
    assert!(fenced.metadata_transfer_fence_source_imported);

    let stale_destination_epoch = next_epoch(authority.snapshot().cluster_epoch()).unwrap();
    let stale_second_transfer = PgMetadataTransferProof::new_with_imported_metadata_proof(
        active_epoch,
        local_progress,
        PgMetadataProof::current(
            2,
            stale_destination_epoch.get(),
            local_progress.state_digest,
        ),
    );
    authority
        .set_pg_acting_set(PgId::new(53), vec![NodeId::new(1)])
        .unwrap();
    let snapshot_after_unrelated_advance = authority.snapshot().clone();
    let actual_destination_epoch = next_epoch(authority.snapshot().cluster_epoch()).unwrap();
    assert!(matches!(
        authority.set_pg_acting_set_with_metadata_transfer_at_epoch(
            pg_id,
            vec![NodeId::new(1)],
            stale_second_transfer,
            stale_destination_epoch,
        ),
        Err(ControlPlaneError::PgMetadataTransferDestinationEpochMismatch {
            pg_id: 52,
            expected_destination_epoch,
            actual_destination_epoch: actual,
        }) if expected_destination_epoch == stale_destination_epoch
            && actual == actual_destination_epoch
    ));
    assert_eq!(authority.snapshot(), &snapshot_after_unrelated_advance);

    let second_transfer = PgMetadataTransferProof::new_with_imported_metadata_proof(
        active_epoch,
        local_progress,
        PgMetadataProof::current(
            2,
            actual_destination_epoch.get(),
            local_progress.state_digest,
        ),
    );
    authority
        .set_pg_acting_set_with_metadata_transfer_at_epoch(
            pg_id,
            vec![NodeId::new(1)],
            second_transfer,
            actual_destination_epoch,
        )
        .unwrap();
    assert_eq!(
        authority.snapshot().cluster_epoch(),
        actual_destination_epoch
    );
    let transferred = authority.snapshot().pg(pg_id).unwrap();
    assert_eq!(transferred.acting_set(), &[NodeId::new(1)]);
    assert_eq!(
        transferred.peering_metadata_transfer(),
        Some(second_transfer)
    );
}

#[test]
fn fenced_metadata_transfer_rejects_epoch_local_source_proof_without_imported_source() {
    let tmp = test_util::tempdir();
    let store = FileControlPlaneStore::new(tmp.path().join("control-plane.state"));
    let mut authority = SingleAuthorityControlPlane::open(store).unwrap();
    for node_id in [1, 2] {
        authority
            .set_node_membership(NodeId::new(node_id), NodeMembershipState::Active)
            .unwrap();
        assert!(heartbeat_until_serving(&mut authority, node_id, 1_000).serving());
    }
    let imported_activation_floor = PgMetadataProof::current(9, 10, 11);
    let epoch_local_source_proof = PgMetadataProof::current(2, 12, 13);
    for (idx, pg_id) in [42, 43].into_iter().enumerate() {
        let base_ms = 1_990 + (idx as u64 * 100);
        authority
            .set_pg_acting_set(PgId::new(pg_id), vec![NodeId::new(1)])
            .unwrap();
        heartbeat_with_pg_proof(
            &mut authority,
            1,
            pg_id,
            PgState::Peering,
            imported_activation_floor,
            false,
            base_ms + 10,
        );
        authority
            .complete_pg_peering(
                PgId::new(pg_id),
                NodeId::new(1),
                node_incarnation(&authority, 1),
                base_ms + 20,
            )
            .unwrap();
        heartbeat_with_pg_proof(
            &mut authority,
            1,
            pg_id,
            PgState::Active,
            imported_activation_floor,
            false,
            base_ms + 30,
        );
    }

    heartbeat_with_pg_proof(
        &mut authority,
        1,
        42,
        PgState::Active,
        imported_activation_floor,
        false,
        2_180,
    );
    authority
        .fence_pg_for_metadata_transfer(PgId::new(42))
        .unwrap();
    assert!(
        !authority
            .snapshot()
            .pg(PgId::new(42))
            .unwrap()
            .metadata_transfer_fence_source_imported
    );
    let fenced_epoch = authority.snapshot().cluster_epoch();
    let fenced_transfer = PgMetadataTransferProof::new_with_imported_metadata_proof(
        fenced_epoch,
        epoch_local_source_proof,
        PgMetadataProof::current(3, 14, 15),
    );
    assert!(matches!(
        authority.set_pg_acting_set_with_metadata_transfer(
            PgId::new(42),
            vec![NodeId::new(2)],
            fenced_transfer,
        ),
        Err(ControlPlaneError::PgMetadataTransferProofBelowFloor { pg_id: 42, .. })
    ));

    authority
        .set_pg_state(PgId::new(43), PgState::Peering)
        .unwrap();
    let unfenced_epoch = authority.snapshot().cluster_epoch();
    let unfenced_transfer = PgMetadataTransferProof::new_with_imported_metadata_proof(
        unfenced_epoch,
        epoch_local_source_proof,
        PgMetadataProof::current(3, 16, 17),
    );
    assert!(matches!(
        authority.set_pg_acting_set_with_metadata_transfer(
            PgId::new(43),
            vec![NodeId::new(2)],
            unfenced_transfer,
        ),
        Err(ControlPlaneError::PgMetadataTransferProofBelowFloor { pg_id: 43, .. })
    ));
}

#[test]
fn fenced_metadata_transfer_accepts_later_prefence_epoch_local_source_proof() {
    let tmp = test_util::tempdir();
    let store = FileControlPlaneStore::new(tmp.path().join("control-plane.state"));
    let mut authority = SingleAuthorityControlPlane::open(store.clone()).unwrap();
    for node_id in [1, 2] {
        authority
            .set_node_membership(NodeId::new(node_id), NodeMembershipState::Active)
            .unwrap();
        assert!(heartbeat_until_serving(&mut authority, node_id, 1_000).serving());
    }

    let pg_id = PgId::new(42);
    let floor = PgMetadataProof::current(3, 9_745, 14_796);
    authority
        .set_pg_acting_set(pg_id, vec![NodeId::new(1)])
        .unwrap();
    heartbeat_with_pg_proof(
        &mut authority,
        1,
        pg_id.get(),
        PgState::Peering,
        floor,
        false,
        2_000,
    );
    authority
        .complete_pg_peering(
            pg_id,
            NodeId::new(1),
            node_incarnation(&authority, 1),
            2_001,
        )
        .unwrap();
    heartbeat_with_pg_proof(
        &mut authority,
        1,
        pg_id.get(),
        PgState::Active,
        floor,
        false,
        2_002,
    );
    let floor_epoch = authority
        .snapshot()
        .pg(pg_id)
        .unwrap()
        .active_metadata_proof_epoch()
        .unwrap();

    authority
        .set_pg_acting_set(PgId::new(43), vec![NodeId::new(1)])
        .unwrap();
    let source_epoch = authority.snapshot().cluster_epoch();
    assert!(source_epoch > floor_epoch);
    heartbeat_with_pg_proof(
        &mut authority,
        1,
        pg_id.get(),
        PgState::Active,
        floor,
        false,
        2_003,
    );
    authority.fence_pg_for_metadata_transfer(pg_id).unwrap();
    let fence_epoch = authority.snapshot().cluster_epoch();
    assert!(fence_epoch > source_epoch);
    assert_eq!(
        authority
            .snapshot()
            .pg(pg_id)
            .unwrap()
            .metadata_transfer_fence_epoch(),
        Some(fence_epoch)
    );

    authority
        .set_pg_acting_set(PgId::new(44), vec![NodeId::new(1)])
        .unwrap();
    let post_fence_epoch = authority.snapshot().cluster_epoch();
    assert!(post_fence_epoch > fence_epoch);
    assert_eq!(
        authority
            .snapshot()
            .pg(pg_id)
            .unwrap()
            .metadata_transfer_fence_epoch(),
        Some(fence_epoch)
    );
    assert_persisted_snapshot_matches_authority(&authority, &store);
    authority = reopen_file_authority(&store);

    let source_proof = PgMetadataProof::current(2, 71_284, 19_648);
    let imported_proof = PgMetadataProof::current(
        source_proof.applied_log_index,
        82_951,
        source_proof.state_digest,
    );
    let stale_transfer = PgMetadataTransferProof::new_with_imported_metadata_proof(
        floor_epoch,
        source_proof,
        imported_proof,
    );
    assert!(matches!(
        authority.set_pg_acting_set_with_metadata_transfer(
            pg_id,
            vec![NodeId::new(2)],
            stale_transfer,
        ),
        Err(ControlPlaneError::PgMetadataTransferProofBelowFloor { pg_id: 42, .. })
    ));

    let at_fence_transfer = PgMetadataTransferProof::new_with_imported_metadata_proof(
        fence_epoch,
        source_proof,
        imported_proof,
    );
    assert!(matches!(
        authority.set_pg_acting_set_with_metadata_transfer(
            pg_id,
            vec![NodeId::new(2)],
            at_fence_transfer,
        ),
        Err(ControlPlaneError::PgMetadataTransferProofBelowFloor { pg_id: 42, .. })
    ));

    let transfer = PgMetadataTransferProof::new_with_imported_metadata_proof(
        source_epoch,
        source_proof,
        imported_proof,
    );
    authority
        .set_pg_acting_set_with_metadata_transfer(pg_id, vec![NodeId::new(2)], transfer)
        .unwrap();
    let transferred = authority.snapshot().pg(pg_id).unwrap();
    assert_eq!(transferred.state(), PgState::Peering);
    assert_eq!(transferred.peering_metadata_transfer(), Some(transfer));
    assert_eq!(
        transferred.peering_metadata_proof_floor(),
        Some(imported_proof)
    );
    assert_persisted_snapshot_matches_authority(&authority, &store);
    let reopened = reopen_file_authority(&store);
    assert_eq!(
        reopened
            .snapshot()
            .pg(pg_id)
            .unwrap()
            .peering_metadata_transfer(),
        Some(transfer)
    );
}

#[test]
fn fenced_metadata_transfer_accepts_prefence_source_epoch_with_floor_proof() {
    let tmp = test_util::tempdir();
    let store = FileControlPlaneStore::new(tmp.path().join("control-plane.state"));
    let mut authority = SingleAuthorityControlPlane::open(store).unwrap();
    for node_id in [1, 2] {
        authority
            .set_node_membership(NodeId::new(node_id), NodeMembershipState::Active)
            .unwrap();
        assert!(heartbeat_until_serving(&mut authority, node_id, 1_000).serving());
    }
    let source_proof = PgMetadataProof::current(3, 9_474, 15_725);
    authority
        .set_pg_acting_set(PgId::new(44), vec![NodeId::new(1)])
        .unwrap();
    heartbeat_with_pg_proof(
        &mut authority,
        1,
        44,
        PgState::Peering,
        source_proof,
        false,
        2_000,
    );
    authority
        .complete_pg_peering(
            PgId::new(44),
            NodeId::new(1),
            node_incarnation(&authority, 1),
            2_001,
        )
        .unwrap();
    heartbeat_with_pg_proof(
        &mut authority,
        1,
        44,
        PgState::Active,
        source_proof,
        false,
        2_002,
    );
    let source_epoch = authority.snapshot().cluster_epoch();

    authority
        .fence_pg_for_metadata_transfer(PgId::new(44))
        .unwrap();
    assert!(source_epoch < authority.snapshot().cluster_epoch());
    let imported_proof = PgMetadataProof::current(3, 49_281, source_proof.state_digest);
    let transfer = PgMetadataTransferProof::new_with_imported_metadata_proof(
        source_epoch,
        source_proof,
        imported_proof,
    );
    authority
        .set_pg_acting_set_with_metadata_transfer(PgId::new(44), vec![NodeId::new(2)], transfer)
        .unwrap();
    let pg = authority.snapshot().pg(PgId::new(44)).unwrap();
    assert_eq!(pg.acting_set(), &[NodeId::new(2)]);
    assert_eq!(pg.peering_metadata_transfer(), Some(transfer));
    assert_eq!(pg.peering_metadata_proof_floor(), Some(imported_proof));
}

#[test]
fn complete_pg_peering_requires_every_acting_node_serving() {
    let tmp = test_util::tempdir();
    let store = FileControlPlaneStore::new(tmp.path().join("control-plane.state"));
    let mut authority = SingleAuthorityControlPlane::open(store).unwrap();
    for node_id in [1, 2] {
        authority
            .set_node_membership(NodeId::new(node_id), NodeMembershipState::Active)
            .unwrap();
    }
    assert!(heartbeat_until_serving(&mut authority, 1, 1_000).serving());
    assert!(heartbeat_until_serving(&mut authority, 2, 1_001).serving());
    authority
        .set_pg_acting_set(PgId::new(39), vec![NodeId::new(1), NodeId::new(2)])
        .unwrap();

    assert!(heartbeat_until_serving(&mut authority, 2, 1_999).serving());
    heartbeat_with_pg_observation(&mut authority, 1, 39, PgState::Peering, 2_000);
    assert!(matches!(
        authority.complete_pg_peering(
            PgId::new(39),
            NodeId::new(1),
            node_incarnation(&authority, 1),
            2_001,
        ),
        Err(ControlPlaneError::PgPeeringMissingObservation {
            pg_id: 39,
            node_id: 2,
            ..
        })
    ));

    heartbeat_with_pg_observation(&mut authority, 2, 39, PgState::Peering, 2_002);
    authority
        .complete_pg_peering(
            PgId::new(39),
            NodeId::new(1),
            node_incarnation(&authority, 1),
            2_003,
        )
        .unwrap();
}

#[test]
fn acting_set_change_discards_stale_peering_observations() {
    let tmp = test_util::tempdir();
    let store = FileControlPlaneStore::new(tmp.path().join("control-plane.state"));
    let mut authority = SingleAuthorityControlPlane::open(store).unwrap();
    for node_id in [1, 2, 3] {
        authority
            .set_node_membership(NodeId::new(node_id), NodeMembershipState::Active)
            .unwrap();
        assert!(heartbeat_until_serving(&mut authority, node_id, 1_000).serving());
    }
    authority
        .set_pg_acting_set(PgId::new(38), vec![NodeId::new(1), NodeId::new(2)])
        .unwrap();
    let first_peering_epoch = authority.snapshot().cluster_epoch();
    heartbeat_with_pg_observation(&mut authority, 1, 38, PgState::Peering, 2_000);
    heartbeat_with_pg_observation(&mut authority, 2, 38, PgState::Peering, 2_001);

    authority
        .set_pg_acting_set(
            PgId::new(38),
            vec![NodeId::new(1), NodeId::new(2), NodeId::new(3)],
        )
        .unwrap();
    let changed_epoch = authority.snapshot().cluster_epoch();
    assert!(changed_epoch > first_peering_epoch);
    assert_eq!(
        authority.snapshot().pg(PgId::new(38)).unwrap().state(),
        PgState::Peering
    );

    assert!(matches!(
        authority.complete_pg_peering(
            PgId::new(38),
            NodeId::new(1),
            node_incarnation(&authority, 1),
            2_002,
        ),
        Err(ControlPlaneError::NodeNotServingCurrentEpoch {
            node_id: 1,
            cluster_epoch,
        }) if cluster_epoch == changed_epoch
    ));
    assert!(authority
        .heartbeat(
            heartbeat_from_record(&authority, 1, changed_epoch, 2_003),
            2_003,
        )
        .unwrap()
        .serving());
    assert!(matches!(
        authority.complete_pg_peering(
            PgId::new(38),
            NodeId::new(1),
            node_incarnation(&authority, 1),
            2_004,
        ),
        Err(ControlPlaneError::PgPeeringMissingObservation {
            pg_id: 38,
            cluster_epoch,
            ..
        }) if cluster_epoch == changed_epoch
    ));

    for (node_id, now_ms) in [(1, 2_005), (2, 2_006), (3, 2_007)] {
        heartbeat_with_pg_observation(&mut authority, node_id, 38, PgState::Peering, now_ms);
    }
    authority
        .complete_pg_peering(
            PgId::new(38),
            NodeId::new(1),
            node_incarnation(&authority, 1),
            2_008,
        )
        .unwrap();
    assert_eq!(
        authority.snapshot().pg(PgId::new(38)).unwrap().state(),
        PgState::Active
    );
}

#[test]
fn membership_change_to_joining_forces_active_pg_to_peering() {
    let tmp = test_util::tempdir();
    let store = FileControlPlaneStore::new(tmp.path().join("control-plane.state"));
    let mut authority = SingleAuthorityControlPlane::open(store).unwrap();
    authority
        .set_node_membership(NodeId::new(1), NodeMembershipState::Active)
        .unwrap();
    assert!(heartbeat_until_serving(&mut authority, 1, 1_000).serving());
    authority
        .set_pg_acting_set(PgId::new(23), vec![NodeId::new(1)])
        .unwrap();
    heartbeat_with_pg_observation(&mut authority, 1, 23, PgState::Peering, 2_000);
    authority
        .complete_pg_peering(
            PgId::new(23),
            NodeId::new(1),
            node_incarnation(&authority, 1),
            2_001,
        )
        .unwrap();
    let active = heartbeat_with_pg_observation(&mut authority, 1, 23, PgState::Active, 2_002);
    let active_epoch = active.cluster_epoch();
    let authorization = authority
        .authorize_pg_operation(
            PgServiceOperation::MetadataWrite,
            PgId::new(23),
            NodeId::new(1),
            node_incarnation(&authority, 1),
            active_epoch,
            2_003,
        )
        .unwrap();

    authority
        .set_node_membership(NodeId::new(1), NodeMembershipState::Joining)
        .unwrap();
    let joining_epoch = authority.snapshot().cluster_epoch();
    assert!(joining_epoch > active_epoch);
    let pg = authority.snapshot().pg(PgId::new(23)).unwrap();
    assert_eq!(pg.state(), PgState::Peering);
    assert_eq!(pg.active_primary(), None);
    assert_eq!(pg.active_metadata_proof(), None);
    assert_eq!(
        pg.peering_metadata_proof_floor(),
        Some(PgMetadataProof::empty())
    );
    assert!(matches!(
        authority.validate_pg_operation_authorization(&authorization, 2_004),
        Err(ControlPlaneError::StaleAuthorizationEpoch {
            cluster_epoch,
            current_epoch,
        }) if cluster_epoch == active_epoch && current_epoch == joining_epoch
    ));

    let joining_lease = authority
        .heartbeat(
            heartbeat_from_record(&authority, 1, joining_epoch, 2_005),
            2_005,
        )
        .unwrap();
    assert!(!joining_lease.serving());
    assert!(matches!(
        authority.authorize_pg_operation(
            PgServiceOperation::MetadataWrite,
            PgId::new(23),
            NodeId::new(1),
            node_incarnation(&authority, 1),
            joining_epoch,
            2_006,
        ),
        Err(ControlPlaneError::NodeNotServingCurrentEpoch {
            node_id: 1,
            cluster_epoch,
        }) if cluster_epoch == joining_epoch
    ));
}

#[test]
fn membership_change_to_draining_forces_repeering_before_service() {
    let tmp = test_util::tempdir();
    let store = FileControlPlaneStore::new(tmp.path().join("control-plane.state"));
    let mut authority = SingleAuthorityControlPlane::open(store).unwrap();
    authority
        .set_node_membership(NodeId::new(1), NodeMembershipState::Active)
        .unwrap();
    assert!(heartbeat_until_serving(&mut authority, 1, 1_000).serving());
    authority
        .set_pg_acting_set(PgId::new(24), vec![NodeId::new(1)])
        .unwrap();
    heartbeat_with_pg_observation(&mut authority, 1, 24, PgState::Peering, 2_000);
    authority
        .complete_pg_peering(
            PgId::new(24),
            NodeId::new(1),
            node_incarnation(&authority, 1),
            2_001,
        )
        .unwrap();
    let active = heartbeat_with_pg_observation(&mut authority, 1, 24, PgState::Active, 2_002);
    let active_epoch = active.cluster_epoch();

    authority
        .set_node_membership(NodeId::new(1), NodeMembershipState::Draining)
        .unwrap();
    let draining_epoch = authority.snapshot().cluster_epoch();
    assert!(draining_epoch > active_epoch);
    assert_eq!(
        authority.snapshot().pg(PgId::new(24)).unwrap().state(),
        PgState::Peering
    );
    let draining_lease = authority
        .heartbeat(
            heartbeat_from_record(&authority, 1, draining_epoch, 2_003),
            2_003,
        )
        .unwrap();
    assert!(
        draining_lease.serving(),
        "draining nodes can still serve after observing the new map"
    );
    assert!(matches!(
        authority.authorize_pg_operation(
            PgServiceOperation::MetadataWrite,
            PgId::new(24),
            NodeId::new(1),
            node_incarnation(&authority, 1),
            draining_epoch,
            2_004,
        ),
        Err(ControlPlaneError::PgNotActive {
            pg_id: 24,
            state: PgState::Peering,
            ..
        })
    ));

    heartbeat_with_pg_observation(&mut authority, 1, 24, PgState::Peering, 2_005);
    authority
        .complete_pg_peering(
            PgId::new(24),
            NodeId::new(1),
            node_incarnation(&authority, 1),
            2_006,
        )
        .unwrap();
    let active_again = heartbeat_with_pg_observation(&mut authority, 1, 24, PgState::Active, 2_007);
    let authorization = authority
        .authorize_pg_operation(
            PgServiceOperation::MetadataWrite,
            PgId::new(24),
            NodeId::new(1),
            node_incarnation(&authority, 1),
            active_again.cluster_epoch(),
            2_008,
        )
        .unwrap();
    assert_eq!(authorization.primary_node_id(), NodeId::new(1));
}

#[test]
fn failed_expiry_persist_does_not_expose_uncommitted_epoch_or_map() {
    let tmp = test_util::tempdir();
    let store = FileControlPlaneStore::new(tmp.path().join("control-plane.state"));
    let mut authority = SingleAuthorityControlPlane::open(store).unwrap();
    authority
        .set_node_membership(NodeId::new(12), NodeMembershipState::Active)
        .unwrap();
    assert!(heartbeat_until_serving(&mut authority, 12, 1_000).serving());
    let committed = authority.snapshot().clone();
    assert_eq!(
        committed.node(NodeId::new(12)).unwrap().availability(),
        NodeAvailabilityState::Healthy
    );

    let failing_store = FailingStore::new(committed.clone());
    let mut restarted = SingleAuthorityControlPlane::open(failing_store).unwrap();
    assert!(restarted
        .heartbeat(
            heartbeat_from_record(&restarted, 12, restarted.snapshot().cluster_epoch(), 1_001,),
            1_001
        )
        .unwrap()
        .serving());
    let visible_before_failure = restarted.snapshot().clone();
    restarted.store.fail_saves();
    assert!(matches!(
        restarted.expire_heartbeat_leases(1_101),
        Err(ControlPlaneError::Io { diagnostic })
            if diagnostic.context() == "test save failure"
    ));
    assert_eq!(restarted.snapshot(), &visible_before_failure);
    assert_eq!(
        restarted.deterministic_pg_primary(PgId::new(1), &[NodeId::new(12)], 1_001),
        Some(NodeId::new(12))
    );
}

#[test]
fn heartbeat_rejects_unknown_removed_and_zero_duration_nodes() {
    let tmp = test_util::tempdir();
    let store = FileControlPlaneStore::new(tmp.path().join("control-plane.state"));
    let mut authority = SingleAuthorityControlPlane::open(store).unwrap();
    assert!(matches!(
        authority.heartbeat(heartbeat(9, ClusterEpoch::INITIAL, 1), 1),
        Err(ControlPlaneError::UnknownNode { node_id: 9 })
    ));

    authority
        .set_node_membership(NodeId::new(9), NodeMembershipState::Removed)
        .unwrap();
    assert!(matches!(
        authority.heartbeat(heartbeat(9, authority.snapshot().cluster_epoch(), 2), 2),
        Err(ControlPlaneError::NodeCannotReceiveLease { node_id: 9, .. })
    ));

    authority
        .set_node_membership(NodeId::new(10), NodeMembershipState::Active)
        .unwrap();
    let mut invalid = heartbeat(10, authority.snapshot().cluster_epoch(), 3);
    invalid.requested_lease_duration_ms = 0;
    assert!(matches!(
        authority.heartbeat(invalid, 3),
        Err(ControlPlaneError::InvalidLeaseDuration)
    ));
}

#[test]
fn heartbeat_rejects_overlong_lease_duration() {
    let tmp = test_util::tempdir();
    let store = FileControlPlaneStore::new(tmp.path().join("control-plane.state"));
    let mut authority = SingleAuthorityControlPlane::open(store).unwrap();
    authority
        .set_node_membership(NodeId::new(10), NodeMembershipState::Active)
        .unwrap();

    let mut invalid = heartbeat(10, authority.snapshot().cluster_epoch(), 3);
    invalid.requested_lease_duration_ms = MAX_HEARTBEAT_LEASE_MS + 1;
    assert!(matches!(
        authority.heartbeat(invalid, 3),
        Err(ControlPlaneError::LeaseDurationTooLong {
            requested_ms,
            max_ms,
        }) if requested_ms == MAX_HEARTBEAT_LEASE_MS + 1
            && max_ms == MAX_HEARTBEAT_LEASE_MS
    ));
}

#[test]
fn primary_selection_requires_unexpired_authority_lease() {
    let tmp = test_util::tempdir();
    let store = FileControlPlaneStore::new(tmp.path().join("control-plane.state"));
    let mut authority = SingleAuthorityControlPlane::open(store).unwrap();
    authority
        .set_node_membership(NodeId::new(42), NodeMembershipState::Active)
        .unwrap();
    let serving = heartbeat_until_serving(&mut authority, 42, 1_000);
    assert!(serving.serving());
    assert_eq!(
        authority.deterministic_pg_primary(
            PgId::new(1),
            &[NodeId::new(42)],
            serving.lease_deadline_ms() - 1,
        ),
        Some(NodeId::new(42))
    );
    assert_eq!(
        authority.deterministic_pg_primary(
            PgId::new(1),
            &[NodeId::new(42)],
            serving.lease_deadline_ms(),
        ),
        None
    );

    authority
        .set_pg_acting_set(PgId::new(21), vec![NodeId::new(42)])
        .unwrap();
    heartbeat_with_pg_observation(&mut authority, 42, 21, PgState::Peering, 1_010);
    authority
        .complete_pg_peering(
            PgId::new(21),
            NodeId::new(42),
            node_incarnation(&authority, 42),
            1_020,
        )
        .unwrap();
    let active = heartbeat_with_pg_observation(&mut authority, 42, 21, PgState::Active, 1_030);
    assert_eq!(
        authority.serving_pg_primary(PgId::new(21), active.lease_deadline_ms() - 1),
        Some(NodeId::new(42))
    );
    assert_eq!(
        authority.serving_pg_primary(PgId::new(21), active.lease_deadline_ms()),
        None
    );
}

#[test]
fn removed_nodes_cannot_rejoin_or_be_marked_healthy() {
    let tmp = test_util::tempdir();
    let store = FileControlPlaneStore::new(tmp.path().join("control-plane.state"));
    let mut authority = SingleAuthorityControlPlane::open(store).unwrap();
    authority
        .set_node_membership(NodeId::new(11), NodeMembershipState::Active)
        .unwrap();
    authority
        .set_node_membership(NodeId::new(11), NodeMembershipState::Removed)
        .unwrap();

    assert!(matches!(
        authority.set_node_membership(NodeId::new(11), NodeMembershipState::Active),
        Err(ControlPlaneError::RemovedNodeCannotRejoin { node_id: 11 })
    ));
    assert!(matches!(
        authority.mark_node_availability(NodeId::new(11), NodeAvailabilityState::Healthy),
        Err(ControlPlaneError::NodeCannotReceiveLease { node_id: 11, .. })
    ));
}
