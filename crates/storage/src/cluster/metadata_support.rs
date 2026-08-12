// Copyright The Argmin Authors.
// SPDX-License-Identifier: Apache-2.0

pub(super) struct DurableBucketWriteReservation {
    node: Arc<dyn crate::node_client::BucketMetadataNodeClient>,
    pg_id: u32,
    record: BucketWriteReservationRecord,
}

pub(super) struct DurableBucketWriteDrain {
    pg_id: u32,
    record: BucketWriteDrainRecord,
}

pub(super) enum DurableBucketDeleteDrainBegin {
    Acquired(DurableBucketWriteDrain),
    AlreadyDeleting,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ReissuedPendingCommandReplicaMatch {
    BelowReplacement,
    MatchesHashChain,
    MissingOrMismatched,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct ReissuedPendingCommandPrimarySummary {
    node_id: NodeId,
    max_log_index: u64,
    applied_log_index: u64,
    applied_log_hash: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct ReissuedPendingCommandReplicaSummary {
    node_id: NodeId,
    max_log_index: u64,
    applied_log_index: u64,
    applied_log_hash: u64,
    replacement_match: ReissuedPendingCommandReplicaMatch,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ReissuedPendingCommandDecision {
    StaleCommandDisplaced,
    ReloadCurrent,
    Conflict { node_id: NodeId, log_index: u64 },
}

enum ReissuePendingMetadataCommandOutcome {
    Reissued(MetadataCommandEnvelope),
    MatchingCurrent(MetadataCommandEnvelope),
    Missing,
    MatchingCurrentConflict {
        command: MetadataCommandEnvelope,
        source: StoreError,
    },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum MetadataCommandPublicationState {
    NotPublished,
    PublicationStarted,
    Witnessed,
    PublicationUnconfirmed,
    IrrevocableUnconfirmed,
    Published,
}

struct HeldPrimaryMetadataCommandObservation {
    node_id: NodeId,
    result: Result<Option<(u64, u64)>, StoreError>,
    abandonment: Result<MetadataCommandAcceptance, StoreError>,
    publication_started: Result<bool, StoreError>,
}

enum HeldPrimaryMetadataCommandSection {
    Active(Box<dyn crate::node_client::MetadataCommandCriticalSection>),
    Recovery(Box<dyn crate::node_client::MetadataCommandRecoveryCriticalSection>),
}

impl HeldPrimaryMetadataCommandSection {
    fn acceptance_until(
        &self,
        command: &MetadataCommandEnvelope,
        deadline: Instant,
    ) -> Result<MetadataCommandAcceptance, StoreError> {
        match self {
            Self::Active(section) => section.metadata_command_acceptance_until(command, deadline),
            Self::Recovery(section) => {
                section.metadata_command_acceptance_until(command, deadline)
            }
        }
    }

    fn applied_metadata_command_log_entry_hashes_until(
        &self,
        command: &MetadataCommandEnvelope,
        deadline: Instant,
    ) -> Result<Option<(u64, u64)>, StoreError> {
        match self {
            Self::Active(section) => {
                section.applied_metadata_command_log_entry_hashes_until(command, deadline)
            }
            Self::Recovery(section) => {
                section.applied_metadata_command_log_entry_hashes_until(command, deadline)
            }
        }
    }

    fn abandonment_acceptance_until(
        &self,
        command: &MetadataCommandEnvelope,
        deadline: Instant,
    ) -> Result<MetadataCommandAcceptance, StoreError> {
        match self {
            Self::Active(section) => {
                section.metadata_command_abandon_acceptance_until(command, deadline)
            }
            Self::Recovery(section) => {
                section.metadata_command_abandon_acceptance_until(command, deadline)
            }
        }
    }

    fn pending_metadata_command_publication_started_until(
        &self,
        command: &MetadataCommandEnvelope,
        deadline: Instant,
    ) -> Result<bool, StoreError> {
        match self {
            Self::Active(section) => {
                section.pending_metadata_command_publication_started_until(command, deadline)
            }
            Self::Recovery(section) => {
                section.pending_metadata_command_publication_started_until(command, deadline)
            }
        }
    }

    fn mark_pending_metadata_command_publication_started_until(
        &self,
        command: &MetadataCommandEnvelope,
        deadline: Instant,
    ) -> Result<(), crate::node_client::MetadataCommandApplyError> {
        match self {
            Self::Active(section) => {
                section.mark_pending_metadata_command_publication_started_until(command, deadline)
            }
            Self::Recovery(section) => {
                section.mark_pending_metadata_command_publication_started_until(command, deadline)
            }
        }
    }

    fn apply_until(
        &self,
        authorized_source: Option<&MetadataCommandEnvelope>,
        abandoned_source: Option<&MetadataCommandEnvelope>,
        command: &MetadataCommandEnvelope,
        deadline: Instant,
    ) -> Result<MetadataCommandReplicaState, crate::node_client::MetadataCommandApplyError> {
        match self {
            Self::Active(section) => {
                section.apply_metadata_command_and_record_until(command, deadline)
            }
            Self::Recovery(section) => section.apply_metadata_command_and_record_for_recovery_until(
                authorized_source.expect("recovery section must retain its authorized source"),
                abandoned_source,
                command,
                deadline,
            ),
        }
    }
}

fn lock_metadata_command_pg_until(
    lock: &std::sync::Mutex<()>,
    deadline: Instant,
) -> Option<std::sync::MutexGuard<'_, ()>> {
    loop {
        if Instant::now() >= deadline {
            return None;
        }
        match lock.try_lock() {
            Ok(guard) => {
                if Instant::now() >= deadline {
                    drop(guard);
                    return None;
                }
                return Some(guard);
            }
            Err(std::sync::TryLockError::Poisoned(error)) => {
                let guard = error.into_inner();
                if Instant::now() >= deadline {
                    drop(guard);
                    return None;
                }
                return Some(guard);
            }
            Err(std::sync::TryLockError::WouldBlock) => {
                let remaining = deadline.checked_duration_since(Instant::now())?;
                std::thread::sleep(remaining.min(Duration::from_millis(1)));
            }
        }
    }
}

#[must_use = "ContenderDrained must restart from a fresh snapshot"]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum SnapshotSensitiveInstallOutcome {
    Installed,
    ContenderDrained,
}

#[must_use = "allocator contention outcomes must restart or continue deliberately"]
enum AllocatorCleanupFreshInstallOutcome {
    Installed(Box<MetadataCommandEnvelope>),
    PendingContenderDrained,
    LogConflictHandled,
}

#[must_use = "RetryAfterContention must restart allocator/cleanup publication"]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum AllocatorCleanupPendingInstallOutcome {
    Installed,
    RetryAfterContention,
}

#[must_use = "terminal-session contention must preserve matching contenders"]
enum TerminalSessionRetryInstallOutcome {
    Installed,
    MatchingContenderVisible(Box<MetadataCommandEnvelope>),
    UnrelatedContenderVisible,
    ContentionWithoutVisibleCommand,
}

#[must_use = "matching-outcome contention must preserve command-owned results"]
enum MatchingOutcomeRetryInstallOutcome {
    Installed,
    MatchingContenderVisible(Box<MetadataCommandEnvelope>),
    UnrelatedContenderVisible,
    ContentionWithoutVisibleCommand,
}

#[must_use = "apply-validated contention outcomes must restart publication deliberately"]
enum ApplyValidatedFreshInstallOutcome {
    Installed(Box<MetadataCommandEnvelope>),
    PendingContenderDrained,
    LogConflictHandled,
}

#[must_use = "RetryAfterContention must restart apply-validated publication"]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ApplyValidatedPendingInstallOutcome {
    Installed,
    RetryAfterContention,
}

enum ObjectPgPendingCommandInstall {
    Installed(MetadataCommandEnvelope),
    Pending(MetadataCommandEnvelope),
    LogConflict { pending_visible: bool },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum BucketWriteReservationDisposition {
    TransferredToCommand,
    ReleaseByCaller,
    PreserveForOwnershipCheckFailure,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum StreamAppendCommandApplyOutcome {
    Applied,
    RetryFromFreshSnapshot,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum StreamAppendPayloadCleanup {
    EagerAllowed,
    ReferenceCheckRequired,
}

struct StreamAppendCommitRequest<'a> {
    bucket: &'a BucketName,
    key: &'a ObjectKey,
    session_id: &'a SessionId,
    segment_index: u32,
    segment_record: &'a StreamUploadSegmentRecord,
    shard_batch: &'a [(&'a ShardKey, WriteAck)],
}

pub enum BucketWriteSnapshotAction<T, E> {
    Release(Result<T, E>),
    TransferredToCommand(Result<T, E>),
}

impl<T, E> BucketWriteSnapshotAction<T, E> {
    pub fn release(result: Result<T, E>) -> Self {
        Self::Release(result)
    }

    pub fn transferred_to_command(result: Result<T, E>) -> Self {
        Self::TransferredToCommand(result)
    }
}

fn decide_reissued_pending_command(
    primary: ReissuedPendingCommandPrimarySummary,
    acting_set_max_log_index: u64,
    current_log_index: u64,
    payload_matches: bool,
    replicas: &[ReissuedPendingCommandReplicaSummary],
) -> ReissuedPendingCommandDecision {
    if !payload_matches {
        return ReissuedPendingCommandDecision::StaleCommandDisplaced;
    }
    if primary.applied_log_index != primary.max_log_index {
        return ReissuedPendingCommandDecision::Conflict {
            node_id: primary.node_id,
            log_index: primary.max_log_index,
        };
    }
    let Some(expected_log_index) = primary.max_log_index.checked_add(1) else {
        return ReissuedPendingCommandDecision::Conflict {
            node_id: primary.node_id,
            log_index: u64::MAX,
        };
    };
    if current_log_index != expected_log_index || acting_set_max_log_index > current_log_index {
        return ReissuedPendingCommandDecision::Conflict {
            node_id: primary.node_id,
            log_index: acting_set_max_log_index.max(current_log_index),
        };
    }
    for replica in replicas {
        if replica.max_log_index < current_log_index {
            if replica.max_log_index != primary.applied_log_index
                || replica.applied_log_index != primary.applied_log_index
                || replica.applied_log_hash != primary.applied_log_hash
            {
                return ReissuedPendingCommandDecision::Conflict {
                    node_id: replica.node_id,
                    log_index: primary.applied_log_index.max(replica.max_log_index),
                };
            }
            continue;
        }
        if replica.replacement_match != ReissuedPendingCommandReplicaMatch::MatchesHashChain {
            return ReissuedPendingCommandDecision::Conflict {
                node_id: replica.node_id,
                log_index: current_log_index,
            };
        }
    }
    ReissuedPendingCommandDecision::ReloadCurrent
}

fn metadata_transfer_destination_proof(
    artifact: &PgMetadataTransferArtifact,
    commands: &[MetadataTransferCommand],
    destination_cluster_epoch: ClusterEpoch,
) -> PgMetadataProof {
    metadata_transfer_destination_proof_for_commands(
        artifact.pg_id,
        commands.len() as u64,
        artifact.proof.state_digest,
        commands,
        destination_cluster_epoch,
    )
}

fn metadata_transfer_destination_proof_for_commands(
    pg_id: PgId,
    applied_log_index: u64,
    state_digest: CanonicalStateDigest,
    commands: &[MetadataTransferCommand],
    destination_cluster_epoch: ClusterEpoch,
) -> PgMetadataProof {
    let mut applied_log_hash = MetadataCommandLogHash::genesis();
    for transfer_command in commands {
        let command = &transfer_command.command;
        applied_log_hash = metadata_command_log_hash(
            destination_cluster_epoch,
            pg_id,
            command.id().log_index(),
            applied_log_hash.value(),
            command.checksum_crc64(),
        );
    }
    PgMetadataProof::from_carriers(
        applied_log_index,
        applied_log_hash,
        state_digest,
    )
}

fn retained_log_export_failure_allows_checkpoint_fallback(
    error: &PgPeeringReconstructionFailure,
) -> bool {
    matches!(
        error,
        PgPeeringReconstructionFailure::Reconstruction(
            PgPeeringReconstructionError::MissingRetainedCommandLogEntry { .. }
                | PgPeeringReconstructionError::MissingRetainedCommandStateProof { .. }
                | PgPeeringReconstructionError::UnreplayableAbandonedCommandLogEntry { .. }
        )
    )
}

fn metadata_transfer_prefix_proof_at_epoch(
    pg_id: PgId,
    state_digest: CanonicalStateDigest,
    commands: &[MetadataTransferCommand],
    destination_cluster_epoch: ClusterEpoch,
) -> PgMetadataProof {
    let mut applied_log_hash = MetadataCommandLogHash::genesis();
    for (index, transfer_command) in commands.iter().enumerate() {
        let log_index = MetadataCommandLogIndex::new((index + 1) as u64)
            .expect("metadata transfer prefix log indexes are non-zero");
        let command = MetadataCommandEnvelope::new(
            MetadataCommandId::new(destination_cluster_epoch, pg_id, log_index),
            transfer_command.command.payload().clone(),
        );
        applied_log_hash = metadata_command_log_hash(
            destination_cluster_epoch,
            pg_id,
            log_index,
            applied_log_hash.value(),
            command.checksum_crc64(),
        );
    }
    PgMetadataProof::from_carriers(
        commands.len() as u64,
        applied_log_hash,
        state_digest,
    )
}

impl StorageCluster {
    fn validated_data_pg(&self, pg_id: PgId) -> Result<DataPgId, StoreError> {
        self.local_map
            .data_pg(pg_id)
            .ok_or(StoreError::ClusterPgNotFound {
                pg_id: pg_id.get(),
                cluster_epoch: self.operation_epoch(),
            })
    }

    pub(crate) fn metadata_transfer_imported_proof_at_epoch(
        artifact: &PgMetadataTransferArtifact,
        destination_epoch: ClusterEpoch,
    ) -> Result<PgMetadataProof, PgMetadataTransferError> {
        let commands = rebase_pg_metadata_transfer_artifact_commands(artifact, destination_epoch)
            .map_err(PgMetadataTransferError::reconstruction)?;
        Ok(metadata_transfer_destination_proof(
            artifact,
            &commands,
            destination_epoch,
        ))
    }
}

enum MetadataTransferImportDestination {
    AlreadyImported(MetadataCommandReplicaState),
    Empty,
    AdoptBase,
    AdoptExisting,
    AdoptPrefix { prefix_len: usize },
}

fn checkpoint_import_resume_prefix_len(
    pg_id: PgId,
    checkpoint_destination_base_proof: PgMetadataProof,
    expected_import_proof: PgMetadataProof,
    current_proof: PgMetadataProof,
    commands: &[MetadataTransferCommand],
    cluster_epoch: ClusterEpoch,
) -> Option<usize> {
    if current_proof == checkpoint_destination_base_proof {
        return Some(0);
    }
    if current_proof == expected_import_proof {
        return Some(commands.len());
    }
    for (index, command) in commands.iter().enumerate() {
        let prefix_len = index + 1;
        let prefix_proof = metadata_transfer_destination_proof_for_commands(
            pg_id,
            prefix_len as u64,
            command.post_state_digest,
            &commands[..prefix_len],
            cluster_epoch,
        );
        if current_proof == prefix_proof {
            return Some(prefix_len);
        }
    }
    None
}

fn classify_metadata_transfer_import_destination(
    metadata_client: &dyn MetadataCommandPeeringNodeClient,
    node_id: NodeId,
    pg_id: PgId,
    cluster_epoch: ClusterEpoch,
    base_import_proof: PgMetadataProof,
    expected_import_proof: PgMetadataProof,
    commands: &[MetadataTransferCommand],
) -> Result<MetadataTransferImportDestination, PgPeeringReconstructionFailure> {
    if metadata_client
        .pending_metadata_command_envelope(pg_id, cluster_epoch)?
        .is_some()
    {
        return Err(PgPeeringReconstructionError::PendingMetadataCommand { node_id }.into());
    }

    let state = metadata_client.metadata_command_replica_state(pg_id)?;
    if state.cluster_epoch != cluster_epoch
        && metadata_client
            .pending_metadata_command_envelope(pg_id, state.cluster_epoch)?
            .is_some()
    {
        return Err(PgPeeringReconstructionError::PendingMetadataCommand { node_id }.into());
    }
    let proof = PgMetadataProof {
        applied_log_index: state.applied_log_index,
        applied_log_hash: state.applied_log_hash,
        state_digest: state.state_digest,
    };
    if state.cluster_epoch == cluster_epoch && proof == expected_import_proof {
        let peering_route =
            metadata_client.open_metadata_command_peering_route(pg_id, cluster_epoch)?;
        let validated =
            peering_route.validate_metadata_command_replay_state_preserving_pending_slot()?;
        let validated_proof = PgMetadataProof {
            applied_log_index: validated.applied_log_index,
            applied_log_hash: validated.applied_log_hash,
            state_digest: validated.state_digest,
        };
        if validated_proof == expected_import_proof {
            return Ok(MetadataTransferImportDestination::AlreadyImported(
                validated,
            ));
        }
    }

    if state.applied_log_index == 0
        && state.applied_log_hash.value() == 0
        && metadata_client.metadata_command_replica_state_can_initialize(pg_id, cluster_epoch)?
    {
        let actual_proof = PgMetadataProof {
            applied_log_index: state.applied_log_index,
            applied_log_hash: state.applied_log_hash,
            state_digest: state.state_digest,
        };
        if actual_proof == base_import_proof {
            return Ok(MetadataTransferImportDestination::Empty);
        }
        return Err(
            PgPeeringReconstructionError::DirtyMetadataTransferDestination {
                node_id,
                pg_id,
                cluster_epoch,
                applied_log_index: state.applied_log_index,
                applied_log_hash: state.applied_log_hash.value(),
                state_digest: state.state_digest.value(),
                expected: base_import_proof,
            }
            .into(),
        );
    }

    if state.state_digest != expected_import_proof.state_digest {
        if let Some(first_command) = commands.first() {
            if state.state_digest == first_command.pre_state_digest {
                let actual_proof = PgMetadataProof {
                    applied_log_index: state.applied_log_index,
                    applied_log_hash: state.applied_log_hash,
                    state_digest: state.state_digest,
                };
                if base_import_proof.applied_log_index == 0
                    && base_import_proof.applied_log_hash.value() == 0
                    && base_import_proof.state_digest == first_command.pre_state_digest
                {
                    let peering_route = metadata_client
                        .open_metadata_command_peering_route(pg_id, state.cluster_epoch)?;
                    let validated = peering_route
                        .validate_metadata_command_replay_state_preserving_pending_slot()?;
                    let validated_proof = PgMetadataProof {
                        applied_log_index: validated.applied_log_index,
                        applied_log_hash: validated.applied_log_hash,
                        state_digest: validated.state_digest,
                    };
                    if validated_proof == actual_proof {
                        return Ok(MetadataTransferImportDestination::AdoptBase);
                    }
                } else if actual_proof == base_import_proof {
                    let peering_route = metadata_client
                        .open_metadata_command_peering_route(pg_id, state.cluster_epoch)?;
                    let validated = peering_route
                        .validate_metadata_command_replay_state_preserving_pending_slot()?;
                    let validated_proof = PgMetadataProof {
                        applied_log_index: validated.applied_log_index,
                        applied_log_hash: validated.applied_log_hash,
                        state_digest: validated.state_digest,
                    };
                    if validated_proof == base_import_proof {
                        return Ok(MetadataTransferImportDestination::AdoptBase);
                    }
                }
                return Err(
                    PgPeeringReconstructionError::DirtyMetadataTransferDestination {
                        node_id,
                        pg_id,
                        cluster_epoch,
                        applied_log_index: state.applied_log_index,
                        applied_log_hash: state.applied_log_hash.value(),
                        state_digest: state.state_digest.value(),
                        expected: base_import_proof,
                    }
                    .into(),
                );
            }
        }
        for (index, command) in commands.iter().enumerate() {
            let prefix_len = index + 1;
            if state.state_digest != command.post_state_digest {
                continue;
            }
            let prefix_proof = metadata_transfer_destination_proof_for_commands(
                pg_id,
                prefix_len as u64,
                command.post_state_digest,
                &commands[..prefix_len],
                cluster_epoch,
            );
            let actual_proof = PgMetadataProof {
                applied_log_index: state.applied_log_index,
                applied_log_hash: state.applied_log_hash,
                state_digest: state.state_digest,
            };
            if state.cluster_epoch == cluster_epoch {
                if actual_proof == prefix_proof {
                    return Ok(MetadataTransferImportDestination::AdoptPrefix { prefix_len });
                }
                return Err(
                    PgPeeringReconstructionError::DirtyMetadataTransferDestination {
                        node_id,
                        pg_id,
                        cluster_epoch,
                        applied_log_index: state.applied_log_index,
                        applied_log_hash: state.applied_log_hash.value(),
                        state_digest: state.state_digest.value(),
                        expected: prefix_proof,
                    }
                    .into(),
                );
            }
            let historical_prefix_proof = metadata_transfer_prefix_proof_at_epoch(
                pg_id,
                command.post_state_digest,
                &commands[..prefix_len],
                state.cluster_epoch,
            );
            if actual_proof == historical_prefix_proof {
                let peering_route = metadata_client
                    .open_metadata_command_peering_route(pg_id, state.cluster_epoch)?;
                let validated = peering_route
                    .validate_metadata_command_replay_state_preserving_pending_slot()?;
                let validated_proof = PgMetadataProof {
                    applied_log_index: validated.applied_log_index,
                    applied_log_hash: validated.applied_log_hash,
                    state_digest: validated.state_digest,
                };
                if validated_proof == historical_prefix_proof {
                    return Ok(MetadataTransferImportDestination::AdoptPrefix { prefix_len });
                }
            }
            return Err(
                PgPeeringReconstructionError::DirtyMetadataTransferDestination {
                    node_id,
                    pg_id,
                    cluster_epoch,
                    applied_log_index: state.applied_log_index,
                    applied_log_hash: state.applied_log_hash.value(),
                    state_digest: state.state_digest.value(),
                    expected: historical_prefix_proof,
                }
                .into(),
            );
        }
        return Err(
            PgPeeringReconstructionError::DirtyMetadataTransferDestination {
                node_id,
                pg_id,
                cluster_epoch,
                applied_log_index: state.applied_log_index,
                applied_log_hash: state.applied_log_hash.value(),
                state_digest: state.state_digest.value(),
                expected: expected_import_proof,
            }
            .into(),
        );
    }

    Ok(MetadataTransferImportDestination::AdoptExisting)
}
