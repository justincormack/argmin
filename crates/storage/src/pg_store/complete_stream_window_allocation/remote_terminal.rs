// Copyright The Argmin Authors.
// SPDX-License-Identifier: Apache-2.0

//! Foreground append retirement cannot detach or reopen its preparation section.
//! Only resolved file-attempt bookkeeping is discarded by the consuming handoff.
use super::*;

#[must_use]
pub(in crate::node_runtime) struct RemoteStreamAppendTerminal<'route> {
    terminal: CompleteTerminalHandoff<'route>,
    identity: crate::node_client::MetadataCommandSessionIdentity,
}

#[must_use]
pub(in crate::node_runtime) struct RemoteStreamAppendWindowFailure<'route> {
    owner: Box<RemoteStreamAppendTerminal<'route>>,
    error: Error,
}

impl<'route> RemoteStreamAppendWindowFailure<'route> {
    pub(in crate::node_runtime) fn error(&self) -> &Error {
        &self.error
    }
    pub(in crate::node_runtime) fn into_terminal(self) -> RemoteStreamAppendTerminal<'route> {
        *self.owner
    }
}

impl<'route> RemoteStreamAppendTerminal<'route> {
    // Only the consuming, all-member-checked publication handoff can mint this.
    pub(super) fn new(
        terminal: CompleteTerminalHandoff<'route>,
        identity: crate::node_client::MetadataCommandSessionIdentity,
    ) -> Self {
        Self { terminal, identity }
    }

    pub(in crate::node_runtime) fn id(&self) -> MetadataCommandId {
        self.terminal.id()
    }
    pub(in crate::node_runtime) fn application(&self, node: NodeId) -> ApplicationObservation {
        self.terminal.application_observation(node)
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
                context: "remote append terminal original cutoff",
            });
        }
        // Session identity cannot change between this check and the underlying
        // operation's locked live check. A concurrent close fails there; neither
        // operation reconnects or substitutes another session.
        session.require_identity(&self.identity)
    }

    #[cfg(test)]
    pub(in crate::node_runtime::pg_store) fn test_cutoff(&self) -> u64 {
        self.terminal
            .owner
            .captured_deadline()
            .portable
            .portable_wall_valid_until_ms
    }

    pub(in crate::node_runtime) fn retire(
        &mut self,
        primary: &UnixStorageNodeClient,
        session: &UnixStorageNodeMetadataCommandSession,
    ) -> Result<(), CompleteRetirementSendError> {
        self.require_session(primary, session)
            .map_err(CompleteRetirementSendError::NotSent)?;
        self.terminal
            .retire_remote(primary, session, self.terminal.owner.work_deadline())
    }

    pub(in crate::node_runtime) fn into_confirmed_window(
        self,
        primary: &UnixStorageNodeClient,
        session: &UnixStorageNodeMetadataCommandSession,
    ) -> Result<ConfirmedStagingWindow<'route>, RemoteStreamAppendWindowFailure<'route>> {
        if let Err(error) = self.require_session(primary, session) {
            return Err(RemoteStreamAppendWindowFailure {
                owner: Box::new(self),
                error: error.into(),
            });
        }
        let deadline = self.terminal.owner.work_deadline();
        let Self { terminal, identity } = self;
        match terminal.into_confirmed_window_remote(
            primary,
            session,
            deadline,
            #[cfg(test)]
            |_| {},
        ) {
            Ok(window) => Ok(window),
            Err(failure) => {
                let StagingWindowHandoffFailure { terminal, error } = failure;
                Err(RemoteStreamAppendWindowFailure {
                    owner: Box::new(Self {
                        terminal: *terminal,
                        identity,
                    }),
                    error,
                })
            }
        }
    }
}
