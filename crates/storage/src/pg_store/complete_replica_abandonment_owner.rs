// Copyright The Argmin Authors.
// SPDX-License-Identifier: Apache-2.0

//! Primary-confirmed replica tombstones. No source extraction, cleanup or
//! pending-slot retirement; partial progress remains in the original holder.

use super::*;
use crate::cluster::EmbeddedApplicationTarget;
use crate::node_runtime::clients::{UnixStorageNodeClient, UnixStorageNodeMetadataCommandSession};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(in crate::node_runtime) enum ReplicaAbandonmentObservation {
    NoAttemptObserved,
    Unconfirmed,
    TombstoneConfirmed,
}

/// Created only below, after primary confirmation and exact section/session
/// binding. Borrowing the target retains primary serialization through effects.
pub(in crate::node_runtime) struct ReplicaAbandonmentPermit<'a, 'route> {
    owner: &'a CompleteFanoutSender<'route>,
    target: &'a EmbeddedApplicationTarget<'a>,
}

impl ReplicaAbandonmentPermit<'_, '_> {
    pub(in crate::node_runtime) fn target(&self) -> &EmbeddedApplicationTarget<'_> {
        self.target
    }

    pub(in crate::node_runtime) fn require_request(
        &self,
        request: &CompletePublicationRequest<'_>,
    ) -> Result<(), StoreError> {
        self.owner.require_authority()?;
        self.target.require_command(
            request.id(),
            request.checksum_crc64(),
            request.command_sha256(),
        )?;
        let expected = self
            .owner
            .sender
            .source
            .request(self.target.node_id())
            .map_err(|_| StoreError::RouteCapabilitySubjectMismatch {
                operation: "replica abandonment retained context",
            })?;
        if request.node_id() != self.target.node_id()
            || request.node_id() == self.owner.route.primary()
            || !self
                .owner
                .route
                .nodes()
                .any(|node| node == request.node_id())
            || request.id() != expected.id()
            || request.command_sha256() != expected.command_sha256()
            || request.topology() != expected.topology()
        {
            return Err(StoreError::RouteCapabilitySubjectMismatch {
                operation: "replica abandonment exact custody",
            });
        }
        Ok(())
    }
}

impl CompleteUnstartedAbandonment<'_> {
    pub(in crate::node_runtime) fn replica_observation(
        &self,
        node: NodeId,
    ) -> ReplicaAbandonmentObservation {
        self.replicas
            .get(&node)
            .copied()
            .unwrap_or(ReplicaAbandonmentObservation::NoAttemptObserved)
    }

    pub(super) fn require_replica(&self, node: NodeId) -> Result<(), StoreError> {
        self.owner.require_authority()?;
        if self.observation != UnstartedAbandonmentObservation::PrimaryTombstoneConfirmed
            || node == self.owner.route.primary()
            || !self.owner.route.nodes().any(|member| member == node)
        {
            return Err(StoreError::RouteCapabilitySubjectMismatch {
                operation: "replica abandonment requires confirmed primary and admitted replica",
            });
        }
        Ok(())
    }

    pub(in crate::node_runtime) fn record_replica_embedded(
        &mut self,
        section: &EmbeddedPublicationSection<'_>,
        node: NodeId,
        deadline: Instant,
    ) -> Result<(), CompletePublicationError> {
        self.record_replica_embedded_inner(
            section,
            node,
            deadline,
            #[cfg(test)]
            |_| Ok(()),
        )
    }

    fn record_replica_embedded_inner(
        &mut self,
        section: &EmbeddedPublicationSection<'_>,
        node: NodeId,
        deadline: Instant,
        #[cfg(test)] hook: impl FnMut(&'static str) -> Result<(), StoreError>,
    ) -> Result<(), CompletePublicationError> {
        self.require_replica(node)?;
        self.owner.route.require_embedded_section(section)?;
        let target = section.application_target(node)?;
        Self::record_target(
            &self.owner,
            &mut self.replicas,
            &target,
            deadline,
            #[cfg(test)]
            hook,
        )
    }

    pub(in crate::node_runtime) fn record_embedded_replica_from_remote_primary(
        &mut self,
        client: &UnixStorageNodeClient,
        session: &UnixStorageNodeMetadataCommandSession,
        node: NodeId,
        deadline: Instant,
    ) -> Result<(), CompletePublicationError> {
        self.require_replica(node)?;
        self.owner.require_primary(client, session)?;
        let deadline = deadline.min(self.owner.work_deadline());
        let target = self
            .owner
            .route
            .embedded_replica_target(client, session, node, deadline)?;
        Self::record_target(
            &self.owner,
            &mut self.replicas,
            &target,
            deadline,
            #[cfg(test)]
            |_| Ok(()),
        )
    }

    fn record_target(
        owner: &CompleteFanoutSender<'_>,
        replicas: &mut std::collections::BTreeMap<NodeId, ReplicaAbandonmentObservation>,
        target: &EmbeddedApplicationTarget<'_>,
        deadline: Instant,
        #[cfg(test)] hook: impl FnMut(&'static str) -> Result<(), StoreError>,
    ) -> Result<(), CompletePublicationError> {
        let node = target.node_id();
        if replicas.get(&node) == Some(&ReplicaAbandonmentObservation::TombstoneConfirmed) {
            return Ok(());
        }
        let request = owner.sender.source.request(node).map_err(|_| {
            StoreError::RouteCapabilitySubjectMismatch {
                operation: "replica abandonment retained request",
            }
        })?;
        let permit = ReplicaAbandonmentPermit { owner, target };
        permit.require_request(&request)?;
        replicas.insert(node, ReplicaAbandonmentObservation::Unconfirmed);
        permit.record_embedded(
            &request,
            deadline,
            #[cfg(test)]
            hook,
        )?;
        replicas.insert(node, ReplicaAbandonmentObservation::TombstoneConfirmed);
        Ok(())
    }

    #[cfg(test)]
    pub(in crate::node_runtime::pg_store) fn test_record_replica_embedded_with_hook(
        &mut self,
        section: &EmbeddedPublicationSection<'_>,
        node: NodeId,
        deadline: Instant,
        hook: impl FnMut(&'static str) -> Result<(), StoreError>,
    ) -> Result<(), CompletePublicationError> {
        self.record_replica_embedded_inner(section, node, deadline, hook)
    }
}
