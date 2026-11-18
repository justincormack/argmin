// Copyright The Argmin Authors.
// SPDX-License-Identifier: Apache-2.0

//! Request-bound pending observations. Retaining an inspected context does not
//! grant recovery admission, installation custody or permission to clean up.

use super::*;
use crate::storage_rpc::{
    complete_pending as wire, StorageRpcOperationDeadline, StorageRpcPayloadError,
};
pub(in crate::node_runtime) use applied::{CompleteAbandonmentQuery, CompleteAbandonmentRequest};
pub(in crate::node_runtime) use applied::{
    CompleteReplicaAbandonmentQuery, CompleteReplicaAbandonmentRequest,
};
pub(in crate::node_runtime) use publication_sender::{
    AbandonedPartInstallation, AbandonedPartInstallationObservation, AbandonedPartPreparation,
    AbandonedPartPreparationError, AbandonedPartPreparationFailure, AbandonedPartPublication,
    AbandonedPartTerminalFailure, AbandonedPartTerminalHandoff, PreparedAbandonedPart,
    PreparedAbandonedPartBorrow,
};
pub(in crate::node_runtime) use publication_sender::{
    AbandonmentHandoffError, AbandonmentHandoffFailure, CompleteAbandonmentHandoff,
    CompleteReplicaAbandonmentAttempt,
};
pub(in crate::node_runtime) use publication_sender::{
    CompleteAbandonmentAttempt, CompleteAbandonmentSendError,
};
pub(in crate::node_runtime) use publication_sender::{
    DirectIntroductionFilePreparation, DirectStagingFileFailure,
};
pub(in crate::node_runtime) use publication_sender::{
    DirectStagingPreparation, DirectStagingSnapshot, DirectTerminalInstallation,
    DirectTerminalInstallationObservation, DirectTerminalInstallationProof,
    DirectTerminalPreparation, StreamAppendInstallation, StreamAppendInstallationObservation,
    StreamAppendInstallationProof, StreamAppendPreparation,
};
pub(in crate::node_runtime) use publication_sender::{
    RemoteStagingAcknowledgementAttempt, StagingAcknowledgementBatch,
};
use std::{sync::OnceLock, time::Instant};

#[allow(dead_code)] // Candidate owning sender; live publication selection remains gated.
#[path = "complete_publication_sender.rs"]
mod publication_sender;
pub(in crate::node_runtime) use publication_sender::ApplicationObservation;
pub(in crate::node_runtime) use publication_sender::PublicationObservation;
pub(in crate::node_runtime) use publication_sender::RemoteDirectStagingFileWriter;
#[cfg(test)]
pub(in crate::node_runtime) use publication_sender::RemoteStreamStagingFileWriter;
pub(in crate::node_runtime) use publication_sender::{
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
pub(in crate::node_runtime) use publication_sender::{
    DirectStagingFileWriter, RemoteDirectFileAttempt, RemoteStreamFileAttempt,
    StreamStagingFileWriter,
};

#[path = "complete_applied_probe.rs"]
mod applied;
#[cfg(test)]
pub(in crate::node_runtime) use applied::CompleteAcceptanceDisposition;
pub(in crate::node_runtime) use applied::CompletePublicationError;
pub(in crate::node_runtime) use applied::CompletePublicationQuery;
pub(in crate::node_runtime) use applied::CompletePublicationTransport;
pub(in crate::node_runtime) use applied::{
    CompleteAbandonedEvidence, CompleteAbandonedQuery, CompleteAcceptanceEvidence,
    CompleteAcceptanceKind, CompleteAcceptanceQuery, CompleteAppliedEvidence, CompleteAppliedQuery,
    CompletePublicationRequest,
};
pub(in crate::node_runtime) use applied::{CompleteRetirementQuery, CompleteRetirementRequest};

pub(in crate::node_runtime) struct CompletePendingRequest<'a> {
    pg_id: PgId,
    epoch: ClusterEpoch,
    topology: &'a PgTopology,
}

/// The complete context cannot be detached into a fresh submission. Future
/// recovery must consume this observation through its own admitted boundary.
pub(in crate::node_runtime) struct CompletePendingEvidence {
    pg_id: PgId,
    epoch: ClusterEpoch,
    topology: PgTopology,
    pending: Option<(ContextualCommand, bool)>,
    publication_deadline: PublicationDeadline,
}

/// Owned by the retained evidence/query, never by a disposable request view.
/// Once initialized, no retry or new projection can renew either clock bound.
#[derive(Default)]
struct PublicationDeadline(OnceLock<CapturedPublicationDeadline>);

#[derive(Clone, Copy)]
struct CapturedPublicationDeadline {
    local: Instant,
    portable: StorageRpcOperationDeadline,
}

impl PublicationDeadline {
    fn received(portable: StorageRpcOperationDeadline) -> Self {
        Self(OnceLock::from(CapturedPublicationDeadline {
            local: portable.local_deadline(),
            portable,
        }))
    }

    fn capture(&self, deadline: Instant) -> CapturedPublicationDeadline {
        *self.0.get_or_init(|| CapturedPublicationDeadline {
            local: deadline,
            portable: StorageRpcOperationDeadline::from_instant(deadline),
        })
    }
}

impl CompletePendingEvidence {
    pub(in crate::node_runtime) fn id(&self) -> Option<MetadataCommandId> {
        self.pending.as_ref().map(|(context, _)| context.id())
    }

    pub(in crate::node_runtime) fn publication_started(&self) -> bool {
        self.pending.as_ref().is_some_and(|(_, started)| *started)
    }

    #[cfg(test)]
    pub(in crate::node_runtime::pg_store) fn context(&self) -> Option<&ContextualCommand> {
        self.pending.as_ref().map(|(context, _)| context)
    }
}

fn invalid(reason: &'static str) -> StorageRpcPayloadError {
    StorageRpcPayloadError::InvalidResponseEnvelope(reason)
}

impl<'a> CompletePendingRequest<'a> {
    pub(in crate::node_runtime) fn new(
        pg_id: PgId,
        epoch: ClusterEpoch,
        topology: &'a PgTopology,
    ) -> Result<Self, StorageRpcPayloadError> {
        if !topology.contains_pg(pg_id) || i64::try_from(epoch.get()).is_err() {
            return Err(invalid("complete pending request scope"));
        }
        Ok(Self {
            pg_id,
            epoch,
            topology,
        })
    }

    pub(in crate::node_runtime) fn read(
        &self,
        pg: &PgStore,
        node_id: u32,
    ) -> Result<CompletePendingEvidence, CompletePendingInspectionError> {
        self.read_slot(pg, node_id, false)
    }

    /// Unlike an epoch-scoped query, a contender inspection must never project
    /// a historical slot as clear. Recovery of that slot needs separate admission.
    pub(in crate::node_runtime) fn read_current_slot(
        &self,
        pg: &PgStore,
        node_id: crate::NodeId,
    ) -> Result<CompletePendingEvidence, CompletePendingInspectionError> {
        self.read_slot(pg, node_id.as_u32(), true)
    }

    fn read_slot(
        &self,
        pg: &PgStore,
        node_id: u32,
        require_current: bool,
    ) -> Result<CompletePendingEvidence, CompletePendingInspectionError> {
        if pg.pg_id != self.pg_id.get() {
            return Err(StoreError::RouteCapabilitySubjectMismatch {
                operation: "read complete pending PG",
            }
            .into());
        }
        let snapshot = pg.command_build_snapshot()?;
        let pending = pg.load_complete_pending_in_open_transaction(node_id, self.topology)?;
        if require_current
            && pending
                .as_ref()
                .is_some_and(|pending| pending.observation.id.cluster_epoch() != self.epoch)
        {
            return Err(StoreError::RouteCapabilitySubjectMismatch {
                operation: "inspect direct contender epoch",
            }
            .into());
        }
        // Validate the actual slot even when it belongs to a different epoch.
        // Absence for this request is not permission to retire that slot.
        let pending = pending
            .filter(|pending| pending.observation.id.cluster_epoch() == self.epoch)
            .map(|pending| (pending.context, pending.observation.publication_started));
        snapshot.finish()?;
        Ok(self.evidence(pending))
    }

    pub(in crate::node_runtime) fn read_response(
        &self,
        pg: &PgStore,
        node_id: u32,
    ) -> Result<Vec<u8>, CompletePendingInspectionError> {
        let evidence = self.read(pg, node_id)?;
        match evidence.pending.as_ref() {
            None => Ok(wire::encode(None, false)?),
            Some((context, started)) => {
                let encoded = context
                    .encode_command()
                    .map_err(|_| invalid("encode complete pending context"))?;
                Ok(wire::encode(
                    Some((encoded.checksum_crc64(), encoded.bytes())),
                    *started,
                )?)
            }
        }
    }

    pub(in crate::node_runtime) fn decode_response(
        &self,
        bytes: &[u8],
    ) -> Result<CompletePendingEvidence, StorageRpcPayloadError> {
        let wire::BorrowedPendingResponse {
            command,
            publication_started,
        } = wire::decode(bytes)?;
        let pending = command
            .map(|(checksum, bytes)| {
                let context = ContextualCommand::decode_command(bytes, checksum, self.topology)
                    .map_err(|_| invalid("decode complete pending context"))?;
                if context.id().pg_id() != self.pg_id || context.id().cluster_epoch() != self.epoch
                {
                    return Err(invalid("complete pending response subject"));
                }
                Ok((context, publication_started))
            })
            .transpose()?;
        Ok(self.evidence(pending))
    }

    fn evidence(&self, pending: Option<(ContextualCommand, bool)>) -> CompletePendingEvidence {
        CompletePendingEvidence {
            pg_id: self.pg_id,
            epoch: self.epoch,
            topology: self.topology.clone(),
            pending,
            publication_deadline: PublicationDeadline::default(),
        }
    }
}
