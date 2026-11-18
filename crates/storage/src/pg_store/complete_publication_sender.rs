// Copyright The Argmin Authors.
// SPDX-License-Identifier: Apache-2.0

//! Owning candidate publication history. This owns inspected evidence or the
//! original submission and its history, not frontend payload custody or recovery
//! admission. No command, encoding,
//! deadline reset, cleanup Drop or reconstructible submission escapes it.

use super::*;
use crate::cluster::EmbeddedPublicationSection;
use crate::node_runtime::clients::UnixStorageNodeMetadataCommandSession;
use crate::node_runtime::pg_store::payload_staging::{
    ContextSubmissionObservation, SubmittedPublicationEvidence,
};
pub(in crate::node_runtime) use fanout::{
    AbandonedPartInstallation, AbandonedPartInstallationObservation, AbandonedPartPreparation,
    AbandonedPartPreparationError, AbandonedPartPreparationFailure, AbandonedPartPublication,
    AbandonedPartTerminalFailure, AbandonedPartTerminalHandoff, PreparedAbandonedPart,
    PreparedAbandonedPartBorrow,
};
pub(in crate::node_runtime) use fanout::{
    AbandonmentHandoffError, AbandonmentHandoffFailure, CompleteAbandonmentHandoff,
    CompleteReplicaAbandonmentAttempt,
};
pub(in crate::node_runtime) use fanout::{
    CompleteAbandonmentAttempt, CompleteAbandonmentSendError,
};
pub(in crate::node_runtime) use fanout::{
    DirectIntroductionFilePreparation, DirectStagingFileFailure,
};
pub(in crate::node_runtime) use fanout::{
    DirectStagingPreparation, DirectStagingSnapshot, DirectTerminalInstallation,
    DirectTerminalInstallationObservation, DirectTerminalInstallationProof,
    DirectTerminalPreparation, StreamAppendInstallation, StreamAppendInstallationObservation,
    StreamAppendInstallationProof, StreamAppendPreparation,
};
pub(in crate::node_runtime) use fanout::{
    RemoteStagingAcknowledgementAttempt, StagingAcknowledgementBatch,
};
use placement::NodeId;
use std::collections::BTreeMap;

#[path = "complete_application_sender.rs"]
mod application;
#[path = "complete_fanout_sender.rs"]
mod fanout;
pub(in crate::node_runtime) use application::{
    ApplicationObservation, CompleteApplicationAttempt, CompleteApplicationSendError,
};
pub(in crate::node_runtime) use fanout::RemoteDirectStagingFileWriter;
#[cfg(test)]
pub(in crate::node_runtime) use fanout::RemoteStreamStagingFileWriter;
pub(in crate::node_runtime) use fanout::{
    AllocatedStreamWindow, CompleteFanoutCompletion, CompleteFanoutSender,
    CompleteRetirementAttempt, CompleteRetirementSendError, CompleteTerminalHandoff,
    CompleteUnstartedAbandonment, ConfirmedStagingWindow, RemoteStreamAllocationAttempt,
    ReplicaAbandonmentObservation, ReplicaAbandonmentPermit, StagingWindowHandoffFailure,
    StreamWindowAllocationFailure, StreamWindowAllocationPermit, TerminalHandoffFailure,
    TerminalReservationAuthority, TerminalReservationObservation, TerminalSlotRetirementAuthority,
    TerminalSlotRetirementObservation, UnstartedAbandonmentAuthority,
    UnstartedAbandonmentObservation,
};
pub(in crate::node_runtime) use fanout::{
    DirectStagingFileWriter, RemoteDirectFileAttempt, RemoteStreamFileAttempt,
    StreamStagingFileWriter,
};

/// Historical diagnostics only, never current apply or cleanup authority.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(in crate::node_runtime) enum PublicationObservation {
    NoAttemptObserved,
    Unconfirmed,
    MarkerConfirmed,
}

/// Classification of this single closed transport operation, not cumulative
/// history. In particular NotSent cannot erase an earlier ambiguous dispatch.
#[derive(Debug, thiserror::Error)]
pub(in crate::node_runtime) enum CompletePublicationSendError {
    #[error("complete publication not sent: {0}")]
    NotSent(StoreError),
    #[error("complete publication unconfirmed: {0}")]
    Unconfirmed(StoreError),
}

#[derive(Debug, thiserror::Error)]
pub(in crate::node_runtime) enum PublicationAttemptError {
    #[error(transparent)]
    Preparation(#[from] StorageRpcPayloadError),
    #[error(transparent)]
    Remote(#[from] CompletePublicationSendError),
    #[error(transparent)]
    Embedded(#[from] CompletePublicationError),
}

pub(in crate::node_runtime) struct CompletePublicationSender {
    source: PublicationSource,
    node_id: NodeId,
    observation: PublicationObservation,
    applications: BTreeMap<NodeId, ApplicationObservation>,
}

enum PublicationSource {
    Inspected(CompletePendingEvidence),
    Submitted {
        evidence: SubmittedPublicationEvidence,
        deadline: PublicationDeadline,
    },
}

impl PublicationSource {
    fn request(
        &self,
        node_id: NodeId,
    ) -> Result<CompletePublicationRequest<'_>, StorageRpcPayloadError> {
        match self {
            Self::Inspected(source) => source.publication_request(node_id, &source.topology),
            Self::Submitted { evidence, deadline } => deadline.submitted_request(evidence, node_id),
        }
    }
}

/// Only the owning sender can lend this attempt to the concrete session. The
/// session cannot be called with detached bytes or a stateless transport instead.
pub(in crate::node_runtime) struct CompletePublicationAttempt<'a> {
    transport: &'a CompletePublicationTransport<'a>,
    observation: &'a mut PublicationObservation,
    previous: PublicationObservation,
}

impl<'a> CompletePublicationAttempt<'a> {
    fn new(
        transport: &'a CompletePublicationTransport<'a>,
        observation: &'a mut PublicationObservation,
    ) -> Self {
        let previous = *observation;
        *observation = previous.after_attempt_started();
        Self {
            transport,
            observation,
            previous,
        }
    }

    pub(in crate::node_runtime) fn transport(&self) -> &CompletePublicationTransport<'_> {
        self.transport
    }

    pub(in crate::node_runtime) fn not_sent(
        self,
        error: StoreError,
    ) -> CompletePublicationSendError {
        *self.observation = self.previous;
        CompletePublicationSendError::NotSent(error)
    }

    pub(in crate::node_runtime) fn unconfirmed(
        self,
        error: StoreError,
    ) -> CompletePublicationSendError {
        CompletePublicationSendError::Unconfirmed(error)
    }

    pub(in crate::node_runtime) fn confirm(
        self,
        bytes: &[u8],
    ) -> Result<(), CompletePublicationSendError> {
        self.transport.decode_confirmation(bytes).map_err(|error| {
            CompletePublicationSendError::Unconfirmed(StoreError::StorageRpc {
                node_id: self.transport.node_id().as_u32(),
                operation: "confirm complete publication",
                failure: crate::storage_rpc::StorageRpcErrorCode::PayloadDecode,
                detail: crate::StorageNodeFailureDetail::new(error.to_string()),
            })
        })?;
        *self.observation = PublicationObservation::MarkerConfirmed;
        Ok(())
    }
}

impl CompletePendingEvidence {
    pub(in crate::node_runtime) fn into_publication_sender(
        self,
        node_id: NodeId,
        deadline: Instant,
    ) -> CompletePublicationSender {
        // Freeze before any attempt, including local preflight failures. Moving
        // evidence whose deadline was already captured never restarts its budget.
        self.publication_deadline.capture(deadline);
        CompletePublicationSender {
            source: PublicationSource::Inspected(self),
            node_id,
            observation: PublicationObservation::NoAttemptObserved,
            applications: BTreeMap::new(),
        }
    }
}

impl CompletePublicationSender {
    #[cfg(test)]
    pub(in crate::node_runtime::pg_store) fn test_captured_cutoff(
        &self,
    ) -> StorageRpcOperationDeadline {
        let deadline = match &self.source {
            PublicationSource::Inspected(evidence) => &evidence.publication_deadline,
            PublicationSource::Submitted { deadline, .. } => deadline,
        };
        deadline
            .0
            .get()
            .expect("sender owns its captured deadline")
            .portable
    }

    pub(in crate::node_runtime::pg_store) fn from_submission(
        evidence: SubmittedPublicationEvidence,
        node_id: NodeId,
        cutoff: Instant,
    ) -> Self {
        Self::from_submission_with_cutoff(
            evidence,
            node_id,
            cutoff,
            StorageRpcOperationDeadline::from_instant(cutoff),
        )
    }

    pub(in crate::node_runtime::pg_store) fn from_submission_with_cutoff(
        evidence: SubmittedPublicationEvidence,
        node_id: NodeId,
        local: Instant,
        portable: StorageRpcOperationDeadline,
    ) -> Self {
        // The caller already owns this captured pair. Do not sample clocks or
        // extend it while moving custody between installation and publication.
        let deadline = PublicationDeadline(OnceLock::from(CapturedPublicationDeadline {
            local,
            portable,
        }));
        Self {
            source: PublicationSource::Submitted { evidence, deadline },
            node_id,
            observation: PublicationObservation::NoAttemptObserved,
            applications: BTreeMap::new(),
        }
    }

    /// Immutable installation history, distinct from publication confirmation.
    /// None means an inspection-only origin, not absence of installation.
    pub(in crate::node_runtime::pg_store) fn installation_observation(
        &self,
    ) -> Option<ContextSubmissionObservation> {
        match &self.source {
            PublicationSource::Inspected(_) => None,
            PublicationSource::Submitted { evidence, .. } => Some(evidence.observation()),
        }
    }

    pub(in crate::node_runtime) fn observation(&self) -> PublicationObservation {
        self.observation
    }

    pub(in crate::node_runtime) fn id(&self) -> Option<MetadataCommandId> {
        match &self.source {
            PublicationSource::Inspected(source) => source.id(),
            PublicationSource::Submitted { evidence, .. } => {
                Some(evidence.observation().command_id)
            }
        }
    }

    pub(in crate::node_runtime) fn send_remote(
        &mut self,
        session: &UnixStorageNodeMetadataCommandSession,
        deadline: Instant,
    ) -> Result<(), PublicationAttemptError> {
        let request = self.source.request(self.node_id)?;
        let transport = request.prepare_transport(deadline);
        // Set before the closed send: panic/unwind must also retain uncertainty.
        // Once confirmed, later errors cannot erase that historical observation.
        session.send_complete_publication(CompletePublicationAttempt::new(
            &transport,
            &mut self.observation,
        ))?;
        Ok(())
    }

    pub(in crate::node_runtime) fn send_embedded(
        &mut self,
        section: &EmbeddedPublicationSection<'_>,
        deadline: Instant,
    ) -> Result<(), PublicationAttemptError> {
        self.send_embedded_inner(
            section,
            deadline,
            #[cfg(test)]
            |_| Ok(()),
        )
    }

    /// Dispatch without releasing the caller's exact primary-section borrow.
    pub(in crate::node_runtime::pg_store) fn send_remote_held(
        &mut self,
        held: &mut crate::node_client::CompletePrimarySessionBorrow<'_>,
        deadline: Instant,
    ) -> Result<(), PublicationAttemptError> {
        let request = self.source.request(self.node_id)?;
        let transport = request.prepare_transport(deadline);
        held.send_complete_publication(CompletePublicationAttempt::new(
            &transport,
            &mut self.observation,
        ))?;
        Ok(())
    }

    fn send_embedded_inner(
        &mut self,
        section: &EmbeddedPublicationSection<'_>,
        deadline: Instant,
        #[cfg(test)] hook: impl FnMut(&'static str) -> Result<(), StoreError>,
    ) -> Result<(), PublicationAttemptError> {
        let request = self.source.request(self.node_id)?;
        // Embedded errors deliberately have no definitive-absence meaning;
        // COMMIT/rollback or a post-COMMIT failure may be ambiguous.
        self.observation = self.observation.after_attempt_started();
        #[cfg(not(test))]
        section.start_complete_publication(&request, deadline)?;
        #[cfg(test)]
        section.test_with_publication_authority(&request, deadline, |pg, authority| {
            request.test_start_embedded_with_hook(pg, authority, hook)
        })?;
        self.observation = PublicationObservation::MarkerConfirmed;
        Ok(())
    }

    #[cfg(test)]
    pub(in crate::node_runtime) fn test_send_embedded_with_hook(
        &mut self,
        section: &EmbeddedPublicationSection<'_>,
        deadline: Instant,
        hook: impl FnMut(&'static str) -> Result<(), StoreError>,
    ) -> Result<(), PublicationAttemptError> {
        self.send_embedded_inner(section, deadline, hook)
    }
}

impl PublicationObservation {
    fn after_attempt_started(self) -> Self {
        match self {
            Self::NoAttemptObserved | Self::Unconfirmed => Self::Unconfirmed,
            Self::MarkerConfirmed => Self::MarkerConfirmed,
        }
    }
}
