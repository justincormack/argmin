// Copyright The Argmin Authors.
// SPDX-License-Identifier: Apache-2.0

//! No reconnect, authority renewal, generic terminal extraction or cleanup API.
use super::*;

#[must_use]
pub(in crate::node_runtime) struct RemoteDirectPutTerminal<'route> {
    terminal: CompleteTerminalHandoff<'route>,
    identity: crate::node_client::MetadataCommandSessionIdentity,
}

impl<'route> RemoteDirectPutTerminal<'route> {
    pub(super) fn new(
        terminal: CompleteTerminalHandoff<'route>,
        identity: crate::node_client::MetadataCommandSessionIdentity,
    ) -> Self {
        Self { terminal, identity }
    }
    pub(in crate::node_runtime) fn id(&self) -> MetadataCommandId {
        self.terminal.id()
    }
    pub(in crate::node_runtime) fn work(&self) -> TerminalWorkObservation {
        self.terminal.work()
    }
    pub(in crate::node_runtime) fn application(&self, node: NodeId) -> ApplicationObservation {
        self.terminal.application_observation(node)
    }
    pub(in crate::node_runtime) fn reservation(&self) -> TerminalReservationObservation {
        self.terminal.reservation_observation()
    }
    pub(in crate::node_runtime) fn retirement(&self) -> TerminalSlotRetirementObservation {
        self.terminal.slot_retirement_observation()
    }
    fn require_session(
        &self,
        primary: &UnixStorageNodeClient,
        session: &UnixStorageNodeMetadataCommandSession,
    ) -> Result<(), StoreError> {
        self.terminal.owner.require_primary(primary, session)?;
        let cutoff = self.terminal.owner.captured_deadline();
        if Instant::now() >= cutoff.local
            || crate::clock::current_time_millis() >= cutoff.portable.portable_wall_valid_until_ms
        {
            return Err(StoreError::OperationDeadlineExceeded {
                context: "remote direct terminal original cutoff",
            });
        }
        session.require_identity(&self.identity)
    }
    pub(in crate::node_runtime) fn release_reservation(
        &mut self,
        primary: &UnixStorageNodeClient,
        session: &UnixStorageNodeMetadataCommandSession,
    ) -> Result<(), crate::BucketSnapshotLoadError> {
        self.require_session(primary, session)?;
        // Bucket-proof release has its own retained bucket-PG route. Still keep
        // the original live object-primary section through that separate effect.
        let _held = session.borrow_complete_primary(
            primary,
            self.id().pg_id(),
            self.id().cluster_epoch(),
            self.terminal.owner.captured_deadline().local,
        )?;
        self.terminal.release_reservation()
    }
    pub(in crate::node_runtime) fn retire(
        &mut self,
        primary: &UnixStorageNodeClient,
        session: &UnixStorageNodeMetadataCommandSession,
    ) -> Result<(), crate::node_runtime::pg_store::CompleteRetirementSendError> {
        self.require_session(primary, session)
            .map_err(crate::node_runtime::pg_store::CompleteRetirementSendError::NotSent)?;
        // Immutable identity is checked above; the closed operation acquires
        // the same session's bounded live lock. It never reconnects.
        self.terminal.retire_remote(
            primary,
            session,
            self.terminal.owner.captured_deadline().local,
        )
    }
    #[cfg(test)]
    pub(in crate::node_runtime::pg_store) fn test_cutoff(&self) -> (Instant, u64) {
        let cutoff = self.terminal.owner.captured_deadline();
        (cutoff.local, cutoff.portable.portable_wall_valid_until_ms)
    }
    #[cfg(test)]
    pub(in crate::node_runtime::pg_store) fn test_installation(
        &self,
    ) -> Option<ContextSubmissionObservation> {
        self.terminal.installation_observation()
    }
}
