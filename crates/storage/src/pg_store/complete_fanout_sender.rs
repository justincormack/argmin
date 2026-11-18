// Copyright The Argmin Authors.
// SPDX-License-Identifier: Apache-2.0

//! Admission-bound cohort and all-member evidence, retaining original custody.
//! Terminal effects require consuming this holder; no pending-slot removal or
//! payload cleanup is implemented here.

use super::application::ApplicationAttemptError;
use super::*;
use crate::cluster::CompleteFanoutRoute;
use crate::metadata_command::MetadataCommandReplicaState;
use crate::node_runtime::clients::UnixStorageNodeClient;
use abandonment::AbandonedPartFinalizerRelease;
pub(in crate::node_runtime) use abandonment::{
    AbandonedPartInstallation, AbandonedPartInstallationObservation, AbandonedPartPreparation,
    AbandonedPartPreparationError, AbandonedPartPreparationFailure, AbandonedPartPublication,
    AbandonedPartTerminalFailure, AbandonedPartTerminalHandoff, PreparedAbandonedPart,
    PreparedAbandonedPartBorrow,
};
pub(in crate::node_runtime) use abandonment::{
    AbandonmentHandoffError, AbandonmentHandoffFailure, CompleteAbandonmentHandoff,
    CompleteReplicaAbandonmentAttempt,
};
pub(in crate::node_runtime) use abandonment::{
    CompleteAbandonmentAttempt, CompleteAbandonmentSendError,
};
pub(in crate::node_runtime) use terminal::{
    DirectIntroductionFilePreparation, DirectStagingFileFailure,
};
pub(in crate::node_runtime) use terminal::{
    DirectStagingPreparation, DirectStagingSnapshot, DirectTerminalInstallation,
    DirectTerminalInstallationObservation, DirectTerminalInstallationProof,
    DirectTerminalPreparation, StreamAppendInstallation, StreamAppendInstallationObservation,
    StreamAppendInstallationProof, StreamAppendPreparation,
};
pub(in crate::node_runtime) use terminal::{
    RemoteStagingAcknowledgementAttempt, StagingAcknowledgementBatch,
};

#[path = "complete_unstarted_abandonment_owner.rs"]
mod abandonment;
pub(in crate::node_runtime) use abandonment::{
    CompleteUnstartedAbandonment, ReplicaAbandonmentObservation, ReplicaAbandonmentPermit,
    UnstartedAbandonmentAuthority, UnstartedAbandonmentObservation,
};

#[path = "complete_terminal_handoff.rs"]
mod terminal;
pub(in crate::node_runtime) use terminal::RemoteDirectStagingFileWriter;
#[cfg(test)]
pub(in crate::node_runtime) use terminal::RemoteStreamStagingFileWriter;
pub(in crate::node_runtime) use terminal::{
    AllocatedStreamWindow, CompleteRetirementAttempt, CompleteRetirementSendError,
    CompleteTerminalHandoff, ConfirmedStagingWindow, RemoteStreamAllocationAttempt,
    StagingWindowHandoffFailure, StreamWindowAllocationFailure, StreamWindowAllocationPermit,
    TerminalHandoffFailure, TerminalReservationAuthority, TerminalReservationObservation,
    TerminalSlotRetirementAuthority, TerminalSlotRetirementObservation,
};
pub(in crate::node_runtime) use terminal::{
    DirectStagingFileWriter, RemoteDirectFileAttempt, RemoteStreamFileAttempt,
    StreamStagingFileWriter,
};

pub(in crate::node_runtime) struct CompleteFanoutSender<'a> {
    route: CompleteFanoutRoute<'a>,
    sender: CompletePublicationSender,
}

/// This borrowed evidence cannot outlive either the admitted cohort or the
/// complete command holder. It is not authority to perform terminal effects.
pub(in crate::node_runtime) struct CompleteFanoutCompletion<'a, 'route> {
    owner: &'a CompleteFanoutSender<'route>,
}

#[derive(Debug, thiserror::Error)]
pub(in crate::node_runtime) enum FanoutError {
    #[error(transparent)]
    Staging(#[from] crate::node_runtime::pg_store::StagingWindowConfirmationError),
    #[error(transparent)]
    Scope(#[from] StoreError),
    #[error(transparent)]
    Publication(#[from] PublicationAttemptError),
    #[error(transparent)]
    Application(#[from] ApplicationAttemptError),
    #[error("complete fanout requires its exact witness confirmation before primary apply")]
    WitnessNotConfirmed,
    #[error(
        "complete fanout requires its exact primary confirmation before trailing replica apply"
    )]
    PrimaryNotConfirmed,
    #[error("complete fanout has confirmed {confirmed} of {required} required peers")]
    Incomplete { confirmed: usize, required: usize },
}

impl CompletePublicationSender {
    pub(in crate::node_runtime::pg_store) fn require_cohort_application(
        &self,
        route: &CompleteFanoutRoute<'_>,
        node: NodeId,
    ) -> Result<(), FanoutError> {
        self.require_cohort_subject(route)?;
        if !route.nodes().any(|member| member == node) {
            return Err(StoreError::RouteCapabilitySubjectMismatch {
                operation: "complete application target is not in the admitted cohort",
            }
            .into());
        }
        require_cohort_application_order(route, self, node)
    }

    fn require_cohort_subject(&self, route: &CompleteFanoutRoute<'_>) -> Result<(), StoreError> {
        route.require_valid()?;
        let mismatch = || StoreError::RouteCapabilitySubjectMismatch {
            operation: "complete fanout requires its original command subject",
        };
        let request = self.source.request(self.node_id).map_err(|_| mismatch())?;
        if self.node_id != route.primary()
            || request.id().pg_id() != route.pg_id()
            || request.id().cluster_epoch() != route.epoch()
            || request.topology() != route.topology()
        {
            return Err(mismatch());
        }
        route.require_command(
            request.id(),
            request.checksum_crc64(),
            request.command_sha256(),
        )
    }

    pub(in crate::node_runtime::pg_store) fn apply_embedded_in_cohort(
        &mut self,
        route: &CompleteFanoutRoute<'_>,
        section: &EmbeddedPublicationSection<'_>,
        node: NodeId,
        deadline: Instant,
        #[cfg(test)] hook: impl FnMut(&'static str) -> Result<(), StoreError>,
    ) -> Result<MetadataCommandReplicaState, FanoutError> {
        self.require_cohort_subject(route)?;
        route.require_embedded_section(section)?;
        let target = section.application_target(node)?;
        require_cohort_application_order(route, self, node)?;
        #[cfg(not(test))]
        let state = self.apply_embedded(&target, deadline)?;
        #[cfg(test)]
        let state = self.test_apply_embedded_with_hook(&target, deadline, hook)?;
        Ok(state)
    }

    pub(in crate::node_runtime::pg_store) fn confirmed_cohort_members(
        &self,
        route: &CompleteFanoutRoute<'_>,
    ) -> Result<usize, FanoutError> {
        self.require_cohort_subject(route)?;
        require_cohort_completion(route, self)?;
        Ok(route.nodes().count())
    }

    pub(in crate::node_runtime::pg_store) fn apply_remote_replica_in_cohort(
        &mut self,
        route: &CompleteFanoutRoute<'_>,
        client: &UnixStorageNodeClient,
        deadline: Instant,
    ) -> Result<MetadataCommandReplicaState, FanoutError> {
        self.require_cohort_subject(route)?;
        route.require_client(client)?;
        if client.node_id() == route.primary() {
            return Err(StoreError::RouteCapabilitySubjectMismatch {
                operation: "complete fanout primary requires its held session",
            }
            .into());
        }
        require_cohort_application_order(route, self, client.node_id())?;
        Ok(self.apply_remote_in_fanout(client, route, deadline)?)
    }

    pub(in crate::node_runtime) fn into_fanout(
        mut self,
        route: CompleteFanoutRoute<'_>,
    ) -> Result<CompleteFanoutSender<'_>, (Box<Self>, StoreError)> {
        // Binding failure returns the original owner, including uncertain
        // installation history. No fresh submission or silent Drop is created.
        let result = (|| {
            route.require_valid()?;
            let request = self.source.request(self.node_id).map_err(|_| {
                StoreError::RouteCapabilitySubjectMismatch {
                    operation: "complete fanout requires retained command custody",
                }
            })?;
            if self.observation != PublicationObservation::NoAttemptObserved
                || !self.applications.is_empty()
                || self.node_id != route.primary()
                || request.id().pg_id() != route.pg_id()
                || request.id().cluster_epoch() != route.epoch()
                || request.topology() != route.topology()
            {
                return Err(StoreError::RouteCapabilitySubjectMismatch {
                    operation:
                        "complete fanout must bind the exact route before publication attempts",
                });
            }
            route.require_command(
                request.id(),
                request.checksum_crc64(),
                request.command_sha256(),
            )?;
            Ok(())
        })();
        if let Err(error) = result {
            return Err((Box::new(self), error));
        }
        let deadline = match &mut self.source {
            PublicationSource::Inspected(evidence) => &mut evidence.publication_deadline,
            PublicationSource::Submitted { deadline, .. } => deadline,
        };
        let captured = deadline
            .0
            .get()
            .expect("sender captures deadline before binding");
        let (local, portable) = route.intersect_deadline(captured.local, captured.portable);
        // Only a consuming, pre-publication bind can narrow the frozen pair.
        // Retry projections cannot call this or replace the admitted cohort.
        *deadline = PublicationDeadline(std::sync::OnceLock::from(CapturedPublicationDeadline {
            local,
            portable,
        }));
        Ok(CompleteFanoutSender {
            route,
            sender: self,
        })
    }
}

impl CompleteFanoutSender<'_> {
    pub(in crate::node_runtime) fn open_primary_session(
        &self,
        client: &UnixStorageNodeClient,
        deadline: Instant,
    ) -> Result<UnixStorageNodeMetadataCommandSession, StoreError> {
        self.require_authority()?;
        let session =
            client.open_complete_fanout_primary(&self.route, deadline.min(self.work_deadline()))?;
        self.require_authority()?;
        Ok(session)
    }

    pub(in crate::node_runtime) fn publish_embedded(
        &mut self,
        section: &EmbeddedPublicationSection<'_>,
        deadline: Instant,
    ) -> Result<(), FanoutError> {
        self.require_authority()?;
        self.route.require_embedded_section(section)?;
        Ok(self.sender.send_embedded(section, deadline)?)
    }

    #[cfg(test)]
    pub(in crate::node_runtime::pg_store) fn test_publish_embedded_with_hook(
        &mut self,
        section: &EmbeddedPublicationSection<'_>,
        deadline: Instant,
        hook: impl FnMut(&'static str) -> Result<(), StoreError>,
    ) -> Result<(), FanoutError> {
        self.require_authority()?;
        self.route.require_embedded_section(section)?;
        Ok(self
            .sender
            .test_send_embedded_with_hook(section, deadline, hook)?)
    }

    pub(in crate::node_runtime) fn apply_embedded(
        &mut self,
        section: &EmbeddedPublicationSection<'_>,
        node: NodeId,
        deadline: Instant,
    ) -> Result<MetadataCommandReplicaState, FanoutError> {
        self.apply_embedded_inner(
            section,
            node,
            deadline,
            #[cfg(test)]
            |_| Ok(()),
        )
    }

    fn apply_embedded_inner(
        &mut self,
        section: &EmbeddedPublicationSection<'_>,
        node: NodeId,
        deadline: Instant,
        #[cfg(test)] hook: impl FnMut(&'static str) -> Result<(), StoreError>,
    ) -> Result<MetadataCommandReplicaState, FanoutError> {
        self.require_authority()?;
        self.sender.apply_embedded_in_cohort(
            &self.route,
            section,
            node,
            deadline,
            #[cfg(test)]
            hook,
        )
    }

    #[cfg(test)]
    pub(in crate::node_runtime::pg_store) fn test_apply_embedded_with_hook(
        &mut self,
        section: &EmbeddedPublicationSection<'_>,
        node: NodeId,
        deadline: Instant,
        hook: impl FnMut(&'static str) -> Result<(), StoreError>,
    ) -> Result<MetadataCommandReplicaState, FanoutError> {
        self.apply_embedded_inner(section, node, deadline, hook)
    }

    fn require_application_order(&self, node: NodeId) -> Result<(), FanoutError> {
        require_cohort_application_order(&self.route, &self.sender, node)
    }

    /// Keep the original admitted route, exact primary session and owning
    /// command/history together while the selected embedded replica applies.
    pub(in crate::node_runtime) fn apply_embedded_replica(
        &mut self,
        primary: &UnixStorageNodeClient,
        session: &UnixStorageNodeMetadataCommandSession,
        node: NodeId,
        deadline: Instant,
    ) -> Result<MetadataCommandReplicaState, FanoutError> {
        self.apply_embedded_replica_inner(
            primary,
            session,
            node,
            deadline,
            #[cfg(test)]
            |_| Ok(()),
        )
    }

    fn apply_embedded_replica_inner(
        &mut self,
        primary: &UnixStorageNodeClient,
        session: &UnixStorageNodeMetadataCommandSession,
        node: NodeId,
        deadline: Instant,
        #[cfg(test)] hook: impl FnMut(&'static str) -> Result<(), StoreError>,
    ) -> Result<MetadataCommandReplicaState, FanoutError> {
        self.require_primary(primary, session)?;
        self.require_application_order(node)?;
        // Preparation must not wait past the immutable holder cutoff before
        // the sender's later request projection gets a chance to enforce it.
        let deadline = deadline.min(self.work_deadline());
        let target = self
            .route
            .embedded_replica_target(primary, session, node, deadline)?;
        #[cfg(not(test))]
        let state = self.sender.apply_embedded(&target, deadline)?;
        #[cfg(test)]
        let state = self
            .sender
            .test_apply_embedded_with_hook(&target, deadline, hook)?;
        Ok(state)
    }

    #[cfg(test)]
    pub(in crate::node_runtime::pg_store) fn test_apply_embedded_replica_with_hook(
        &mut self,
        primary: &UnixStorageNodeClient,
        session: &UnixStorageNodeMetadataCommandSession,
        node: NodeId,
        deadline: Instant,
        hook: impl FnMut(&'static str) -> Result<(), StoreError>,
    ) -> Result<MetadataCommandReplicaState, FanoutError> {
        self.apply_embedded_replica_inner(primary, session, node, deadline, hook)
    }

    fn work_deadline(&self) -> Instant {
        self.captured_deadline().local
    }

    fn captured_deadline(&self) -> CapturedPublicationDeadline {
        let deadline = match &self.sender.source {
            PublicationSource::Inspected(evidence) => &evidence.publication_deadline,
            PublicationSource::Submitted { deadline, .. } => deadline,
        };
        deadline
            .0
            .get()
            .copied()
            .expect("bound sender retains deadline")
    }

    fn require_authority(&self) -> Result<(), StoreError> {
        self.route.require_valid()?;
        if Instant::now() >= self.work_deadline() {
            return Err(StoreError::OperationDeadlineExceeded {
                context: "complete fanout original work cutoff",
            });
        }
        Ok(())
    }

    fn require_primary(
        &self,
        client: &UnixStorageNodeClient,
        session: &UnixStorageNodeMetadataCommandSession,
    ) -> Result<(), StoreError> {
        self.require_authority()?;
        self.route.require_client(client)?;
        if client.node_id() != self.route.primary() {
            return Err(StoreError::RouteCapabilitySubjectMismatch {
                operation: "complete fanout primary client",
            });
        }
        session.require_origin(client)
    }

    pub(in crate::node_runtime) fn publish(
        &mut self,
        client: &UnixStorageNodeClient,
        session: &UnixStorageNodeMetadataCommandSession,
        deadline: Instant,
    ) -> Result<(), FanoutError> {
        self.require_primary(client, session)?;
        Ok(self.sender.send_remote(session, deadline)?)
    }

    pub(in crate::node_runtime) fn apply_primary(
        &mut self,
        client: &UnixStorageNodeClient,
        session: &UnixStorageNodeMetadataCommandSession,
        deadline: Instant,
    ) -> Result<MetadataCommandReplicaState, FanoutError> {
        self.require_primary(client, session)?;
        self.require_application_order(self.route.primary())?;
        Ok(self.sender.apply_remote_in_session(session, deadline)?)
    }

    pub(in crate::node_runtime) fn apply_replica(
        &mut self,
        client: &UnixStorageNodeClient,
        deadline: Instant,
    ) -> Result<MetadataCommandReplicaState, FanoutError> {
        self.require_authority()?;
        self.sender
            .apply_remote_replica_in_cohort(&self.route, client, deadline)
    }

    fn witness(&self) -> Option<NodeId> {
        self.route
            .nodes()
            .filter(|node| *node != self.route.primary())
            .min()
    }

    pub(in crate::node_runtime) fn completion(
        &self,
    ) -> Result<CompleteFanoutCompletion<'_, '_>, FanoutError> {
        self.require_authority()?;
        require_cohort_completion(&self.route, &self.sender)?;
        Ok(CompleteFanoutCompletion { owner: self })
    }

    pub(in crate::node_runtime) fn id(&self) -> MetadataCommandId {
        self.sender.id().expect("bound command identity")
    }

    pub(in crate::node_runtime) fn application_observation(
        &self,
        node: NodeId,
    ) -> ApplicationObservation {
        self.sender.application_observation(node)
    }

    #[cfg(test)]
    pub(in crate::node_runtime::pg_store) fn installation_observation(
        &self,
    ) -> Option<ContextSubmissionObservation> {
        self.sender.installation_observation()
    }

    #[cfg(test)]
    pub(in crate::node_runtime) fn test_attempt(
        &mut self,
        node: NodeId,
        deadline: Instant,
        inspect: impl FnOnce(&[u8], Instant),
    ) {
        self.sender
            .test_application_attempt(node, deadline, inspect)
            .unwrap_err();
    }
}

// Shared all-member evidence check; callers separately validate their retained
// route and immutable cutoffs before minting any terminal holder.
fn require_cohort_completion(
    route: &CompleteFanoutRoute<'_>,
    sender: &CompletePublicationSender,
) -> Result<(), FanoutError> {
    let required = route.nodes().count();
    let confirmed = route
        .nodes()
        .filter(|node| sender.application_observation(*node) == ApplicationObservation::Applied)
        .count();
    if sender.observation != PublicationObservation::MarkerConfirmed || confirmed != required {
        return Err(FanoutError::Incomplete {
            confirmed,
            required,
        });
    }
    Ok(())
}

// Shared by owning fanout and the append holder which retains its original
// cohort inside physical custody. No route clone or separate admission needed.
fn require_cohort_application_order(
    route: &CompleteFanoutRoute<'_>,
    sender: &CompletePublicationSender,
    node: NodeId,
) -> Result<(), FanoutError> {
    let witness = route.nodes().filter(|peer| *peer != route.primary()).min();
    if node == route.primary() {
        if witness.is_some_and(|peer| {
            sender.application_observation(peer) != ApplicationObservation::Applied
        }) {
            return Err(FanoutError::WitnessNotConfirmed);
        }
    } else if Some(node) != witness
        && sender.application_observation(route.primary()) != ApplicationObservation::Applied
    {
        return Err(FanoutError::PrimaryNotConfirmed);
    }
    Ok(())
}

impl CompleteFanoutCompletion<'_, '_> {
    pub(in crate::node_runtime) fn id(&self) -> MetadataCommandId {
        self.owner.id()
    }
    pub(in crate::node_runtime) fn confirmed_peers(&self) -> usize {
        self.owner.route.nodes().count()
    }
    pub(in crate::node_runtime) fn require_valid(&self) -> Result<(), StoreError> {
        self.owner.require_authority()
    }
}
