// Copyright The Argmin Authors.
// SPDX-License-Identifier: Apache-2.0

//! Read-only pending custody inspection for coordinated startup integration.
//! This is not full startup validation, replay, convergence or cleanup authority.
//! It never clears pending custody, repairs indexes or reconstructs a submission.

use super::*;
use crate::PgTopology;
use payload_staging::ContextualCommand;
pub(in crate::node_runtime) use transport::{
    AbandonedPartInstallation, AbandonedPartInstallationObservation, AbandonedPartPreparation,
    AbandonedPartPreparationError, AbandonedPartPreparationFailure, AbandonedPartPublication,
    AbandonedPartTerminalFailure, AbandonedPartTerminalHandoff, PreparedAbandonedPart,
    PreparedAbandonedPartBorrow,
};
pub(in crate::node_runtime) use transport::{
    AbandonmentHandoffError, AbandonmentHandoffFailure, CompleteAbandonmentHandoff,
    CompleteReplicaAbandonmentAttempt,
};
pub(in crate::node_runtime) use transport::{
    CompleteAbandonmentAttempt, CompleteAbandonmentSendError,
};
pub(in crate::node_runtime) use transport::{CompleteAbandonmentQuery, CompleteAbandonmentRequest};
pub(in crate::node_runtime) use transport::{
    CompleteReplicaAbandonmentQuery, CompleteReplicaAbandonmentRequest,
};
pub(in crate::node_runtime) use transport::{
    DirectIntroductionFilePreparation, DirectStagingFileFailure,
};
pub(in crate::node_runtime) use transport::{
    DirectStagingPreparation, DirectStagingSnapshot, DirectTerminalInstallation,
    DirectTerminalInstallationObservation, DirectTerminalInstallationProof,
    DirectTerminalPreparation, StreamAppendInstallation, StreamAppendInstallationObservation,
    StreamAppendInstallationProof, StreamAppendPreparation,
};
pub(in crate::node_runtime) use transport::{
    RemoteStagingAcknowledgementAttempt, StagingAcknowledgementBatch,
};

#[path = "complete_pending_transport.rs"]
mod transport;
pub(in crate::node_runtime) use transport::ApplicationObservation;
#[cfg(test)]
pub(in crate::node_runtime) use transport::CompleteAcceptanceDisposition;
pub(in crate::node_runtime) use transport::CompletePublicationError;
pub(in crate::node_runtime) use transport::CompletePublicationQuery;
pub(super) use transport::CompletePublicationTransport;
pub(in crate::node_runtime) use transport::PublicationObservation;
pub(in crate::node_runtime) use transport::RemoteDirectStagingFileWriter;
#[cfg(test)]
pub(in crate::node_runtime) use transport::RemoteStreamStagingFileWriter;
pub(in crate::node_runtime) use transport::{
    AllocatedStreamWindow, CompleteApplicationAttempt, CompleteApplicationSendError,
    CompleteFanoutCompletion, CompleteFanoutSender, CompletePublicationAttempt,
    CompletePublicationSendError, CompletePublicationSender, CompleteRetirementAttempt,
    CompleteRetirementSendError, CompleteTerminalHandoff, CompleteUnstartedAbandonment,
    ConfirmedStagingWindow, RemoteStreamAllocationAttempt, ReplicaAbandonmentObservation,
    ReplicaAbandonmentPermit, StagingWindowHandoffFailure, StreamWindowAllocationFailure,
    StreamWindowAllocationPermit, TerminalHandoffFailure, TerminalReservationAuthority,
    TerminalReservationObservation, TerminalSlotRetirementAuthority,
    TerminalSlotRetirementObservation, UnstartedAbandonmentAuthority,
    UnstartedAbandonmentObservation,
};
pub(in crate::node_runtime) use transport::{
    CompleteAbandonedEvidence, CompleteAbandonedQuery, CompleteAcceptanceEvidence,
    CompleteAcceptanceKind, CompleteAcceptanceQuery, CompleteAppliedEvidence, CompleteAppliedQuery,
    CompletePendingEvidence, CompletePendingRequest, CompletePublicationRequest,
};
pub(in crate::node_runtime) use transport::{CompleteRetirementQuery, CompleteRetirementRequest};
pub(in crate::node_runtime) use transport::{
    DirectStagingFileWriter, RemoteDirectFileAttempt, RemoteStreamFileAttempt,
    StreamStagingFileWriter,
};

#[derive(Debug, thiserror::Error)]
pub(in crate::node_runtime) enum CompletePendingInspectionError {
    #[error(transparent)]
    Store(#[from] StoreError),
    #[error(transparent)]
    Snapshot(#[from] MetadataError),
    #[error(transparent)]
    Response(#[from] crate::storage_rpc::StorageRpcPayloadError),
}

/// Scalar diagnostics only. In particular, `Absent` is the absence of local
/// terminal evidence, NOT proof that no participant accepted publication.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(in super::super) struct CompletePendingObservation {
    pub(in super::super) id: MetadataCommandId,
    pub(in super::super) stored_epoch: ClusterEpoch,
    pub(in super::super) publication_started: bool,
    pub(in super::super) terminal: MetadataCommandStartupDisposition,
}

struct RetainedPending {
    context: ContextualCommand,
    observation: CompletePendingObservation,
}

impl PgStore {
    pub(in super::super) fn inspect_complete_pending_command(
        &self,
        node_id: u32,
        topology: &PgTopology,
    ) -> Result<Option<CompletePendingObservation>, CompletePendingInspectionError> {
        self.require_usable_connection()?;
        if !topology.contains_pg(PgId::new(self.pg_id)) {
            return Err(StoreError::RouteCapabilitySubjectMismatch {
                operation: "inspect complete pending topology",
            }
            .into());
        }
        let snapshot = self.command_build_snapshot()?;
        let observation = self.inspect_complete_pending_in_open_transaction(node_id, topology)?;
        snapshot.finish()?;
        Ok(observation)
    }

    // Shared only with the complete startup transaction. No caller-selected
    // projection, slot clearing, or transaction ownership is exposed.
    pub(super) fn inspect_complete_pending_in_open_transaction(
        &self,
        node_id: u32,
        topology: &PgTopology,
    ) -> Result<Option<CompletePendingObservation>, StoreError> {
        Ok(self
            .load_complete_pending_in_open_transaction(node_id, topology)?
            .map(|pending| pending.observation))
    }

    // Retain the context privately for transport from this exact validated
    // snapshot. Do not reload the slot after validating its indexes/terminal.
    fn load_complete_pending_in_open_transaction(
        &self,
        node_id: u32,
        topology: &PgTopology,
    ) -> Result<Option<RetainedPending>, StoreError> {
        self.require_usable_connection()?;
        if self.conn.is_autocommit() || !topology.contains_pg(PgId::new(self.pg_id)) {
            return Err(StoreError::RouteCapabilitySubjectMismatch {
                operation: "inspect complete pending transaction/topology",
            });
        }
        let state = self.metadata_command_replica_state()?;
        let mut context = None;
        let slot =
            self.pending_metadata_command_slot_with_validator(node_id, |bytes, checksum, id| {
                let conflict = || StoreError::MetadataCommandLogConflict {
                    node_id,
                    pg_id: self.pg_id,
                    cluster_epoch: id.cluster_epoch(),
                    log_index: id.log_index().get(),
                };
                let decoded = ContextualCommand::decode_command(bytes, checksum, topology)
                    .map_err(|_| conflict())?;
                if decoded.id() != id {
                    return Err(conflict());
                }
                context = Some(decoded);
                Ok(())
            })?;
        let observation = if let Some(slot) = slot {
            let context = context.expect("slot validator retains the complete context");
            let encoded =
                context
                    .encode_command()
                    .map_err(|_| StoreError::MetadataCommandLogConflict {
                        node_id,
                        pg_id: self.pg_id,
                        cluster_epoch: slot.id.cluster_epoch(),
                        log_index: slot.id.log_index().get(),
                    })?;
            encoded.require_retained_pending_indexes(self, node_id, topology)?;
            let terminal = encoded.inspect_terminal_in_open_transaction(self, node_id)?;
            // A same-epoch missing terminal can only occupy the next slot. An
            // older/newer epoch stays explicitly unresolved for route-aware
            // recovery; this observation does not decide whether it may apply.
            if terminal == MetadataCommandStartupDisposition::Absent
                && slot.id.cluster_epoch() == state.cluster_epoch
                && state.applied_log_index.checked_add(1) != Some(slot.id.log_index().get())
            {
                return Err(StoreError::MetadataCommandLogConflict {
                    node_id,
                    pg_id: self.pg_id,
                    cluster_epoch: slot.id.cluster_epoch(),
                    log_index: slot.id.log_index().get(),
                });
            }
            Some(RetainedPending {
                context,
                observation: CompletePendingObservation {
                    id: slot.id,
                    stored_epoch: state.cluster_epoch,
                    publication_started: slot.publication_started,
                    terminal,
                },
            })
        } else {
            None
        };
        Ok(observation)
    }
}
