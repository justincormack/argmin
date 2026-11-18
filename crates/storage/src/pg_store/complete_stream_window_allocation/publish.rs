// Copyright The Argmin Authors.
// SPDX-License-Identifier: Apache-2.0

//! The append sender cannot escape its acknowledged physical custody. Only
//! publication/application retain the preparation section identity. Remote
//! terminal transfer consumes into a separately sealed session-bound holder.
use super::*;

#[path = "terminal.rs"]
mod terminal;

#[must_use]
pub(in crate::node_runtime) struct AcknowledgedStreamAppendPublication<'route> {
    custody: AcknowledgedStreamStagingSegment<'route>,
    sender: CompletePublicationSender,
    remote_session: Option<crate::node_client::MetadataCommandSessionIdentity>,
}

#[must_use]
pub(in crate::node_runtime) struct StreamAppendPublicationFailure<'route> {
    owner: Box<AcknowledgedStreamAppendSubmission<'route>>,
    error: Error,
}

impl<'route> StreamAppendPublicationFailure<'route> {
    pub(in crate::node_runtime) fn error(&self) -> &Error {
        &self.error
    }
    pub(in crate::node_runtime) fn into_submission(
        self,
    ) -> AcknowledgedStreamAppendSubmission<'route> {
        *self.owner
    }
}

impl<'route> AcknowledgedStreamAppendSubmission<'route> {
    /// No reinspection, new route, renewed cutoff or metadata effect. Expired
    /// evidence remains retainable; every subsequent effect checks authority.
    pub(in crate::node_runtime) fn into_publication(
        self,
    ) -> Result<AcknowledgedStreamAppendPublication<'route>, StreamAppendPublicationFailure<'route>>
    {
        if self.installation() != StreamAppendInstallationObservation::Installed {
            return Err(StreamAppendPublicationFailure {
                owner: Box::new(self),
                error: Error::Window("append publication requires confirmed installation".into()),
            });
        }
        let PreparedAcknowledgedStreamAppend {
            custody,
            captured,
            remote_session,
        } = self.prepared;
        let writer = custody.writer();
        let original = &writer.allocated.window.terminal.owner;
        let mut cutoff = original.captured_deadline();
        cutoff.local = cutoff.local.min(writer.deadline);
        if let Some(fence) = writer.fence.deadline() {
            cutoff.portable.portable_wall_valid_until_ms = cutoff
                .portable
                .portable_wall_valid_until_ms
                .min(fence.portable_wall_valid_until_ms());
        }
        if let Custody::Remote(remote) = &custody.custody {
            cutoff.portable.portable_wall_valid_until_ms = cutoff
                .portable
                .portable_wall_valid_until_ms
                .min(remote.cutoff.portable_wall_valid_until_ms);
        }
        let mut sender = captured.into_publication_sender(original.route.primary(), cutoff.local);
        // Retain the original absolute wire bound as well as its monotonic
        // bound. Recomputing a duration on handoff could extend authority after
        // a clock change. This is private, consuming, pre-attempt initialization.
        let PublicationSource::Submitted { deadline, .. } = &mut sender.source else {
            unreachable!()
        };
        *deadline = PublicationDeadline(std::sync::OnceLock::from(cutoff));
        Ok(AcknowledgedStreamAppendPublication {
            custody,
            sender,
            remote_session,
        })
    }
}

impl AcknowledgedStreamAppendPublication<'_> {
    pub(in crate::node_runtime) fn id(&self) -> MetadataCommandId {
        self.sender
            .id()
            .expect("append sender retains its complete command")
    }
    pub(in crate::node_runtime) fn installation(&self) -> StreamAppendInstallationObservation {
        // Only confirmed installation can cross the consuming boundary.
        StreamAppendInstallationObservation::Installed
    }
    pub(in crate::node_runtime) fn publication(&self) -> PublicationObservation {
        self.sender.observation()
    }
    pub(in crate::node_runtime) fn application(&self, node: NodeId) -> ApplicationObservation {
        self.sender.application_observation(node)
    }
    fn require_section(&self, section: &EmbeddedPublicationSection<'_>) -> Result<(), FanoutError> {
        if self.remote_session.is_some() {
            return Err(
                Error::Window("remote append requires its original remote section".into()).into(),
            );
        }
        StreamAppendPreparation {
            custody: &self.custody,
        }
        .require_section(section)?;
        Ok(())
    }
    pub(in crate::node_runtime) fn publish_remote(
        &mut self,
        client: &UnixStorageNodeClient,
        session: &UnixStorageNodeMetadataCommandSession,
    ) -> Result<(), FanoutError> {
        let mut held = self.borrow_remote_primary(client, session)?;
        self.sender
            .send_remote_held(&mut held, self.custody.writer().deadline)?;
        Ok(())
    }
    fn borrow_remote_primary<'session>(
        &self,
        client: &UnixStorageNodeClient,
        session: &'session UnixStorageNodeMetadataCommandSession,
    ) -> Result<crate::node_client::CompletePrimarySessionBorrow<'session>, FanoutError> {
        let identity = self
            .remote_session
            .as_ref()
            .ok_or_else(|| Error::Window("remote append requires remote preparation".into()))?;
        let writer = self.custody.writer();
        writer
            .allocated
            .window
            .terminal
            .owner
            .require_primary(client, session)?;
        let held = session.borrow_complete_primary(
            client,
            self.id().pg_id(),
            self.id().cluster_epoch(),
            writer.deadline,
        )?;
        held.require_identity(identity)?;
        StreamAppendPreparation {
            custody: &self.custody,
        }
        .require_valid()?;
        // Keep the validated section borrowed through dispatch. No reconnect,
        // caller-selected cutoff, detached sender or history reset is possible.
        Ok(held)
    }
    pub(in crate::node_runtime) fn apply_remote(
        &mut self,
        primary: &UnixStorageNodeClient,
        session: &UnixStorageNodeMetadataCommandSession,
        target: &UnixStorageNodeClient,
    ) -> Result<MetadataCommandReplicaState, FanoutError> {
        let mut held = self.borrow_remote_primary(primary, session)?;
        let writer = self.custody.writer();
        let route = &writer.allocated.window.terminal.owner.route;
        route.require_client(target)?;
        require_cohort_application_order(route, &self.sender, target.node_id())?;
        let state = if target.node_id() == route.primary() {
            // The primary target must be the exact client that owns the held
            // section, not a second client for the same endpoint/node.
            session.require_origin(target)?;
            self.sender.apply_remote_held(&mut held, writer.deadline)?
        } else {
            // Keep primary transport/shutdown excluded during replica RPC.
            // This is retained section custody, not a remote-liveness promise.
            self.sender
                .apply_remote_in_fanout(target, route, writer.deadline)?
        };
        Ok(state)
    }
    pub(in crate::node_runtime) fn publish_embedded(
        &mut self,
        section: &EmbeddedPublicationSection<'_>,
    ) -> Result<(), FanoutError> {
        self.publish_inner(
            section,
            #[cfg(test)]
            |_| Ok(()),
        )
    }
    fn publish_inner(
        &mut self,
        section: &EmbeddedPublicationSection<'_>,
        #[cfg(test)] hook: impl FnMut(&'static str) -> Result<(), StoreError>,
    ) -> Result<(), FanoutError> {
        self.require_section(section)?;
        let deadline = self.custody.writer().deadline;
        #[cfg(not(test))]
        self.sender.send_embedded(section, deadline)?;
        #[cfg(test)]
        self.sender
            .test_send_embedded_with_hook(section, deadline, hook)?;
        Ok(())
    }
    pub(in crate::node_runtime) fn apply_embedded(
        &mut self,
        section: &EmbeddedPublicationSection<'_>,
        node: NodeId,
    ) -> Result<MetadataCommandReplicaState, FanoutError> {
        self.apply_inner(
            section,
            node,
            #[cfg(test)]
            |_| Ok(()),
        )
    }

    pub(in crate::node_runtime) fn apply_embedded_replica(
        &mut self,
        primary: &UnixStorageNodeClient,
        session: &UnixStorageNodeMetadataCommandSession,
        node: NodeId,
    ) -> Result<MetadataCommandReplicaState, FanoutError> {
        self.apply_embedded_replica_inner(
            primary,
            session,
            node,
            #[cfg(test)]
            |_| Ok(()),
        )
    }

    fn apply_embedded_replica_inner(
        &mut self,
        primary: &UnixStorageNodeClient,
        session: &UnixStorageNodeMetadataCommandSession,
        node: NodeId,
        #[cfg(test)] hook: impl FnMut(&'static str) -> Result<(), StoreError>,
    ) -> Result<MetadataCommandReplicaState, FanoutError> {
        let identity = self
            .remote_session
            .as_ref()
            .ok_or_else(|| Error::Window("remote append requires remote preparation".into()))?;
        let writer = self.custody.writer();
        let owner = &writer.allocated.window.terminal.owner;
        owner.require_primary(primary, session)?;
        require_cohort_application_order(&owner.route, &self.sender, node)?;
        let target = owner.route.embedded_replica_target_in_session(
            primary,
            session,
            identity,
            node,
            writer.deadline,
        )?;
        // Recheck after acquiring the primary mutex. The target retains that
        // exact borrow through local PG admission, SQL commit and confirmation.
        StreamAppendPreparation {
            custody: &self.custody,
        }
        .require_valid()?;
        #[cfg(not(test))]
        let state = self.sender.apply_embedded(&target, writer.deadline)?;
        #[cfg(test)]
        let state = self
            .sender
            .test_apply_embedded_with_hook(&target, writer.deadline, hook)?;
        Ok(state)
    }

    #[cfg(test)]
    pub(in crate::node_runtime::pg_store) fn test_apply_embedded_replica(
        &mut self,
        primary: &UnixStorageNodeClient,
        session: &UnixStorageNodeMetadataCommandSession,
        node: NodeId,
        hook: impl FnMut(&'static str) -> Result<(), StoreError>,
    ) -> Result<MetadataCommandReplicaState, FanoutError> {
        self.apply_embedded_replica_inner(primary, session, node, hook)
    }
    fn apply_inner(
        &mut self,
        section: &EmbeddedPublicationSection<'_>,
        node: NodeId,
        #[cfg(test)] hook: impl FnMut(&'static str) -> Result<(), StoreError>,
    ) -> Result<MetadataCommandReplicaState, FanoutError> {
        self.require_section(section)?;
        let writer = self.custody.writer();
        let route = &writer.allocated.window.terminal.owner.route;
        require_cohort_application_order(route, &self.sender, node)?;
        let target = section.application_target(node)?;
        #[cfg(not(test))]
        let state = self.sender.apply_embedded(&target, writer.deadline)?;
        #[cfg(test)]
        let state = self
            .sender
            .test_apply_embedded_with_hook(&target, writer.deadline, hook)?;
        Ok(state)
    }
    #[cfg(test)]
    pub(in crate::node_runtime::pg_store) fn test_publish(
        &mut self,
        section: &EmbeddedPublicationSection<'_>,
        hook: impl FnMut(&'static str) -> Result<(), StoreError>,
    ) -> Result<(), FanoutError> {
        self.publish_inner(section, hook)
    }
    #[cfg(test)]
    pub(in crate::node_runtime::pg_store) fn test_apply(
        &mut self,
        section: &EmbeddedPublicationSection<'_>,
        node: NodeId,
        hook: impl FnMut(&'static str) -> Result<(), StoreError>,
    ) -> Result<MetadataCommandReplicaState, FanoutError> {
        self.apply_inner(section, node, hook)
    }
    #[cfg(test)]
    pub(in crate::node_runtime::pg_store) fn test_binding_state(
        &self,
    ) -> (Instant, Option<ContextSubmissionObservation>) {
        self.custody.test_binding_state()
    }
    #[cfg(test)]
    pub(in crate::node_runtime::pg_store) fn test_append_installation(
        &self,
    ) -> Option<ContextSubmissionObservation> {
        self.sender.installation_observation()
    }
    #[cfg(test)]
    pub(in crate::node_runtime::pg_store) fn test_cutoff(&self) -> (Instant, u64) {
        let PublicationSource::Submitted { deadline, .. } = &self.sender.source else {
            unreachable!()
        };
        let captured = deadline.0.get().unwrap();
        (
            captured.local,
            captured.portable.portable_wall_valid_until_ms,
        )
    }
}
