// Copyright The Argmin Authors.
// SPDX-License-Identifier: Apache-2.0

//! Owning Direct PUT publication. No independent sender, raw terminal extraction,
//! deadline renewal or physical cleanup; failed attempts retain all custody.
use super::*;

#[path = "retire.rs"]
mod terminal;
pub(in crate::node_runtime) use terminal::{
    DirectPutTerminal, DirectPutTerminalFailure, RemoteDirectPutTerminal,
};

#[must_use]
pub(in crate::node_runtime) struct DirectTerminalPublication<'route> {
    custody: WitnessedDirectPayload<'route>,
    sender: CompletePublicationSender,
}

#[must_use]
pub(in crate::node_runtime) struct DirectTerminalPublicationFailure<'route> {
    owner: Box<DirectTerminalSubmission<'route>>,
    error: Error,
}

impl<'route> DirectTerminalPublicationFailure<'route> {
    pub(in crate::node_runtime) fn error(&self) -> &Error {
        &self.error
    }
    pub(in crate::node_runtime) fn into_submission(self) -> DirectTerminalSubmission<'route> {
        *self.owner
    }
}

impl<'route> DirectTerminalSubmission<'route> {
    /// Transfer confirmed installation, without a storage effect or re-admission.
    /// Expired custody remains retainable; every subsequent effect checks it.
    pub(in crate::node_runtime) fn into_publication(
        self,
    ) -> Result<DirectTerminalPublication<'route>, DirectTerminalPublicationFailure<'route>> {
        if self.installation() != DirectTerminalInstallationObservation::Installed {
            return Err(DirectTerminalPublicationFailure {
                owner: Box::new(self),
                error: Error::Window("direct publication requires confirmed installation".into()),
            });
        }
        let PreparedDirectTerminal { custody, command } = self.prepared;
        let writer = custody.prepared.custody.writer();
        let original = &writer.window.terminal.owner;
        let mut cutoff = original.captured_deadline();
        cutoff.local = cutoff.local.min(writer.deadline);
        if let Some(fence) = writer.fence.deadline() {
            cutoff.portable.portable_wall_valid_until_ms = cutoff
                .portable
                .portable_wall_valid_until_ms
                .min(fence.portable_wall_valid_until_ms());
        }
        if let Custody::Remote(remote) = &custody.prepared.custody.custody {
            cutoff.portable.portable_wall_valid_until_ms = cutoff
                .portable
                .portable_wall_valid_until_ms
                .min(remote.cutoff.portable_wall_valid_until_ms);
        }
        let mut sender = command.into_publication_sender(original.route.primary(), cutoff.local);
        // Preserve both original bounds. Recomputing a remaining duration here
        // could extend the absolute authority after a wall-clock adjustment.
        let PublicationSource::Submitted { deadline, .. } = &mut sender.source else {
            unreachable!()
        };
        *deadline = PublicationDeadline(std::sync::OnceLock::from(cutoff));
        Ok(DirectTerminalPublication { custody, sender })
    }
}

impl DirectTerminalPublication<'_> {
    pub(in crate::node_runtime) fn require_embedded_progress(
        &self,
        section: &EmbeddedPublicationSection<'_>,
    ) -> Result<(), FanoutError> {
        self.require_section(section)?;
        section.require_effect()?;
        Ok(())
    }
    pub(in crate::node_runtime) fn require_remote_progress(
        &self,
        primary: &UnixStorageNodeClient,
        session: &UnixStorageNodeMetadataCommandSession,
    ) -> Result<(), FanoutError> {
        drop(self.borrow_remote_primary(primary, session)?);
        Ok(())
    }
    /// Observation only; callers must use the closed effect methods below.
    /// Pick the witness before the primary, then deterministic trailing peers.
    pub(in crate::node_runtime) fn next_application(&self) -> Option<NodeId> {
        let route = &self
            .custody
            .prepared
            .custody
            .writer()
            .window
            .terminal
            .owner
            .route;
        let unapplied = |node| self.application(node) != ApplicationObservation::Applied;
        if let Some(witness) = route.nodes().filter(|node| *node != route.primary()).min() {
            if unapplied(witness) {
                return Some(witness);
            }
        }
        if unapplied(route.primary()) {
            return Some(route.primary());
        }
        route.nodes().filter(|node| unapplied(*node)).min()
    }
    pub(in crate::node_runtime) fn id(&self) -> MetadataCommandId {
        self.sender
            .id()
            .expect("direct sender retains its complete command")
    }
    pub(in crate::node_runtime) fn installation(&self) -> DirectTerminalInstallationObservation {
        DirectTerminalInstallationObservation::Installed
    }
    pub(in crate::node_runtime) fn publication(&self) -> PublicationObservation {
        self.sender.observation()
    }
    pub(in crate::node_runtime) fn application(&self, node: NodeId) -> ApplicationObservation {
        self.sender.application_observation(node)
    }
    fn require_section(&self, section: &EmbeddedPublicationSection<'_>) -> Result<(), FanoutError> {
        if self.custody.prepared.custody.preparation_session.is_some() {
            return Err(Error::Window(
                "remote direct publication requires its original remote section".into(),
            )
            .into());
        }
        DirectStagingPreparation {
            custody: &self.custody.prepared.custody,
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
            .send_remote_held(&mut held, self.custody.prepared.custody.writer().deadline)?;
        Ok(())
    }
    fn borrow_remote_primary<'session>(
        &self,
        client: &UnixStorageNodeClient,
        session: &'session UnixStorageNodeMetadataCommandSession,
    ) -> Result<crate::node_client::CompletePrimarySessionBorrow<'session>, FanoutError> {
        let writer = self.custody.prepared.custody.writer();
        writer
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
        DirectStagingPreparation {
            custody: &self.custody.prepared.custody,
        }
        .require_remote_session(&held)?;
        // Keep the original section borrowed through dispatch. The sender and
        // immutable cutoff remain inseparable from the acknowledged payload.
        Ok(held)
    }
    pub(in crate::node_runtime) fn apply_remote(
        &mut self,
        primary: &UnixStorageNodeClient,
        session: &UnixStorageNodeMetadataCommandSession,
        target: &UnixStorageNodeClient,
    ) -> Result<MetadataCommandReplicaState, FanoutError> {
        let mut held = self.borrow_remote_primary(primary, session)?;
        let writer = self.custody.prepared.custody.writer();
        let route = &writer.window.terminal.owner.route;
        route.require_client(target)?;
        require_cohort_application_order(route, &self.sender, target.node_id())?;
        let state = if target.node_id() == route.primary() {
            session.require_origin(target)?;
            self.sender.apply_remote_held(&mut held, writer.deadline)?
        } else {
            // Retain the original primary-section borrow until the replica's
            // exact confirmation is classified. This excludes local concurrent
            // transport/shutdown, not independent remote-primary failure.
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
        let custody = &self.custody.prepared.custody;
        let identity = custody.preparation_session.as_ref().ok_or_else(|| {
            Error::Window("remote direct application requires remote preparation".into())
        })?;
        let writer = custody.writer();
        let owner = &writer.window.terminal.owner;
        owner.require_primary(primary, session)?;
        require_cohort_application_order(&owner.route, &self.sender, node)?;
        let target = owner.route.embedded_replica_target_in_session(
            primary,
            session,
            identity,
            node,
            writer.deadline,
        )?;
        // The target retains the exact primary borrow through local PG admission,
        // transaction commit and confirmation. Recheck after acquisition too.
        DirectStagingPreparation { custody }.require_valid()?;
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
    fn publish_inner(
        &mut self,
        section: &EmbeddedPublicationSection<'_>,
        #[cfg(test)] hook: impl FnMut(&'static str) -> Result<(), StoreError>,
    ) -> Result<(), FanoutError> {
        self.require_section(section)?;
        let deadline = self.custody.prepared.custody.writer().deadline;
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
    fn apply_inner(
        &mut self,
        section: &EmbeddedPublicationSection<'_>,
        node: NodeId,
        #[cfg(test)] hook: impl FnMut(&'static str) -> Result<(), StoreError>,
    ) -> Result<MetadataCommandReplicaState, FanoutError> {
        self.require_section(section)?;
        let writer = self.custody.prepared.custody.writer();
        require_cohort_application_order(&writer.window.terminal.owner.route, &self.sender, node)?;
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
    pub(in crate::node_runtime::pg_store) fn test_installation(
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
