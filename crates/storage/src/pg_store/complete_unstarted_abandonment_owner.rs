// Copyright The Argmin Authors.
// SPDX-License-Identifier: Apache-2.0

//! Consuming, admitted custody for unstarted abandonment. Primary confirmation
//! gates replica attempts; neither confirmation nor Drop releases resources,
//! removes a slot or exports a source.

use super::*;
pub(in crate::node_runtime) use handoff::AbandonedPartFinalizerRelease;
pub(in crate::node_runtime) use handoff::{
    AbandonedPartInstallation, AbandonedPartInstallationObservation, AbandonedPartPreparation,
    AbandonedPartPreparationError, AbandonedPartPreparationFailure, AbandonedPartPublication,
    AbandonedPartTerminalFailure, AbandonedPartTerminalHandoff, PreparedAbandonedPart,
    PreparedAbandonedPartBorrow,
};

#[path = "complete_abandonment_handoff.rs"]
mod handoff;
pub(in crate::node_runtime) use handoff::{AbandonmentHandoffFailure, CompleteAbandonmentHandoff};

#[derive(Debug, thiserror::Error)]
pub(in crate::node_runtime) enum AbandonmentHandoffError {
    #[error(transparent)]
    Scope(#[from] StoreError),
    #[error("complete abandonment has confirmed {confirmed} of {required} required peers")]
    Incomplete { confirmed: usize, required: usize },
}

#[path = "complete_unstarted_abandonment_sender.rs"]
mod remote;
#[path = "complete_replica_abandonment_sender.rs"]
mod remote_replicas;
#[path = "complete_replica_abandonment_owner.rs"]
mod replicas;
pub(in crate::node_runtime) use remote::{
    CompleteAbandonmentAttempt, CompleteAbandonmentSendError,
};
pub(in crate::node_runtime) use remote_replicas::CompleteReplicaAbandonmentAttempt;
pub(in crate::node_runtime) use replicas::{
    ReplicaAbandonmentObservation, ReplicaAbandonmentPermit,
};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(in crate::node_runtime) enum UnstartedAbandonmentObservation {
    NoAttemptObserved,
    Unconfirmed,
    PrimaryTombstoneConfirmed,
}

pub(in crate::node_runtime) struct CompleteUnstartedAbandonment<'a> {
    owner: CompleteFanoutSender<'a>,
    observation: UnstartedAbandonmentObservation,
    replicas: std::collections::BTreeMap<NodeId, ReplicaAbandonmentObservation>,
}

/// Only the consuming owner below can lend this permit. Publication authority
/// alone cannot abandon a command; the original owner and admission stay live.
pub(in crate::node_runtime) struct UnstartedAbandonmentAuthority<'a, 'route> {
    owner: &'a CompleteFanoutSender<'route>,
}

impl UnstartedAbandonmentAuthority<'_, '_> {
    pub(in crate::node_runtime) fn require_request(
        &self,
        request: &CompletePublicationRequest<'_>,
    ) -> Result<(), StoreError> {
        self.owner.require_authority()?;
        let expected = self
            .owner
            .sender
            .source
            .request(self.owner.route.primary())
            .map_err(|_| StoreError::RouteCapabilitySubjectMismatch {
                operation: "unstarted abandonment retained command",
            })?;
        if request.id() != expected.id()
            || request.node_id() != expected.node_id()
            || request.command_sha256() != expected.command_sha256()
            || request.topology() != expected.topology()
        {
            return Err(StoreError::RouteCapabilitySubjectMismatch {
                operation: "unstarted abandonment exact command",
            });
        }
        Ok(())
    }

    pub(in crate::node_runtime) fn require_embedded_section(
        &self,
        section: &EmbeddedPublicationSection<'_>,
    ) -> Result<(), StoreError> {
        self.owner.route.require_embedded_section(section)
    }
}

impl<'a> CompleteFanoutSender<'a> {
    pub(in crate::node_runtime) fn into_unstarted_abandonment(
        self,
    ) -> Result<CompleteUnstartedAbandonment<'a>, (Box<Self>, StoreError)> {
        let result = self.require_authority().and_then(|()| {
            if self.sender.observation != PublicationObservation::NoAttemptObserved
                || !self.sender.applications.is_empty()
                || matches!(&self.sender.source, PublicationSource::Inspected(evidence) if evidence.publication_started())
            {
                return Err(StoreError::RouteCapabilitySubjectMismatch {
                    operation:
                        "unstarted abandonment cannot discard publication/application history",
                });
            }
            Ok(())
        });
        if let Err(error) = result {
            return Err((Box::new(self), error));
        }
        Ok(CompleteUnstartedAbandonment {
            owner: self,
            observation: UnstartedAbandonmentObservation::NoAttemptObserved,
            replicas: Default::default(),
        })
    }
}

impl CompleteUnstartedAbandonment<'_> {
    pub(in crate::node_runtime) fn observation(&self) -> UnstartedAbandonmentObservation {
        self.observation
    }

    #[cfg(test)]
    pub(in crate::node_runtime::pg_store) fn installation_observation(
        &self,
    ) -> Option<ContextSubmissionObservation> {
        self.owner.installation_observation()
    }

    pub(in crate::node_runtime) fn record_primary_embedded(
        &mut self,
        section: &EmbeddedPublicationSection<'_>,
        deadline: Instant,
    ) -> Result<(), CompletePublicationError> {
        self.record_primary_embedded_inner(
            section,
            deadline,
            #[cfg(test)]
            |_| Ok(()),
        )
    }

    fn record_primary_embedded_inner(
        &mut self,
        section: &EmbeddedPublicationSection<'_>,
        deadline: Instant,
        #[cfg(test)] hook: impl FnMut(&'static str) -> Result<(), StoreError>,
    ) -> Result<(), CompletePublicationError> {
        self.owner.require_authority()?;
        self.owner.route.require_embedded_section(section)?;
        if self.observation == UnstartedAbandonmentObservation::PrimaryTombstoneConfirmed {
            return Ok(());
        }
        let request = self
            .owner
            .sender
            .source
            .request(self.owner.route.primary())
            .map_err(|_| StoreError::RouteCapabilitySubjectMismatch {
                operation: "unstarted abandonment retained request",
            })?;
        let custody = UnstartedAbandonmentAuthority { owner: &self.owner };
        // Conservatively retain uncertainty on errors and unwind, including an
        // unconfirmed COMMIT. A later pre-send failure cannot erase this history.
        self.observation = UnstartedAbandonmentObservation::Unconfirmed;
        section.abandon_unstarted_complete_publication(
            &request,
            &custody,
            deadline,
            #[cfg(test)]
            hook,
        )?;
        self.observation = UnstartedAbandonmentObservation::PrimaryTombstoneConfirmed;
        Ok(())
    }

    #[cfg(test)]
    pub(in crate::node_runtime::pg_store) fn test_record_primary_embedded_with_hook(
        &mut self,
        section: &EmbeddedPublicationSection<'_>,
        deadline: Instant,
        hook: impl FnMut(&'static str) -> Result<(), StoreError>,
    ) -> Result<(), CompletePublicationError> {
        self.record_primary_embedded_inner(section, deadline, hook)
    }
}
