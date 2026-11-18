// Copyright The Argmin Authors.
// SPDX-License-Identifier: Apache-2.0

//! Consuming all-member handoff. The original owner remains private: no reverse
//! conversion, detached sender, resource release or physical authority.
use super::*;
use crate::node_runtime::pg_store::TerminalSlotRetirementObservation as Retirement;

#[path = "payload_staging_direct_introduction_sender_window.rs"]
mod window;
pub(in crate::node_runtime::pg_store) use window::{
    RemoteIntroductionFileScope, RemoteIntroductionWindowScope,
};

pub(in crate::node_runtime::pg_store) struct RemoteIntroductionRetirementScope<'a, 'route> {
    owner: &'a RemoteDirectIntroductionPublication<'route>,
    retirement: &'a mut Retirement,
}

impl<'a, 'route> RemoteIntroductionRetirementScope<'a, 'route> {
    pub(in crate::node_runtime::pg_store) fn require_session(
        &self,
        held: &crate::node_client::CompletePrimarySessionBorrow<'_>,
    ) -> Result<(), StoreError> {
        self.owner.binding.require_session(held)
    }
    pub(in crate::node_runtime::pg_store) fn into_parts(
        self,
    ) -> (
        &'a CompletePublicationSender,
        &'a crate::cluster::CompleteFanoutRoute<'route>,
        Instant,
        &'a mut Retirement,
    ) {
        (
            &self.owner.sender,
            self.owner.binding.route,
            self.owner.binding.deadline,
            self.retirement,
        )
    }
}

#[must_use = "retain introduction custody until terminal work completes"]
pub(in crate::node_runtime) struct RemoteDirectIntroductionTerminal<'route> {
    owner: RemoteDirectIntroductionPublication<'route>,
    retirement: Retirement,
}

#[must_use = "failed handoff retains the original publication and its history"]
pub(in crate::node_runtime) struct RemoteDirectIntroductionTerminalFailure<'route> {
    owner: Box<RemoteDirectIntroductionPublication<'route>>,
    error: StagingWindowConfirmationError,
}

impl<'route> RemoteDirectIntroductionPublication<'route> {
    pub(in crate::node_runtime) fn into_terminal(
        self,
    ) -> Result<
        RemoteDirectIntroductionTerminal<'route>,
        RemoteDirectIntroductionTerminalFailure<'route>,
    > {
        if let Err(error) = self.require_complete() {
            return Err(RemoteDirectIntroductionTerminalFailure {
                owner: Box::new(self),
                error,
            });
        }
        // Validate without dispatch or storage access, then move the SAME
        // session-bound owner. No clocks are sampled to construct new deadlines
        // and no confirmation observations are reconstructed independently.
        Ok(RemoteDirectIntroductionTerminal {
            owner: self,
            retirement: Retirement::NoAttemptObserved,
        })
    }
}

impl RemoteDirectIntroductionTerminal<'_> {
    pub(in crate::node_runtime) fn retirement(&self) -> Retirement {
        self.retirement
    }
    pub(in crate::node_runtime) fn retire(&mut self) -> Result<(), StagingWindowConfirmationError> {
        let mut held = self.owner.binding.borrow_primary()?;
        CompletePublicationSender::retire_remote_direct_introduction(
            RemoteIntroductionRetirementScope {
                owner: &self.owner,
                retirement: &mut self.retirement,
            },
            &mut held,
        )
    }
    pub(in crate::node_runtime) fn id(&self) -> MetadataCommandId {
        self.owner.id()
    }
    pub(in crate::node_runtime) fn require_valid(
        &self,
    ) -> Result<(), StagingWindowConfirmationError> {
        self.owner.require_complete().map(|_| ())
    }
    /// Historical observations are not renewed route authority.
    pub(in crate::node_runtime) fn application(&self, node: NodeId) -> ApplicationObservation {
        self.owner.application(node)
    }
    pub(in crate::node_runtime) fn installation(&self) -> DirectIntroductionInstallation {
        self.owner.installation()
    }
    pub(in crate::node_runtime) fn publication(&self) -> PublicationObservation {
        self.owner.publication()
    }
    #[cfg(test)]
    pub(in crate::node_runtime::pg_store) fn test_bounds(&self) -> (Instant, u64) {
        self.owner.test_bounds()
    }
    #[cfg(test)]
    pub(in crate::node_runtime::pg_store) fn test_installation(
        &self,
    ) -> submission::InstallationObservation {
        self.owner.test_installation()
    }
}

impl<'route> RemoteDirectIntroductionTerminalFailure<'route> {
    pub(in crate::node_runtime) fn error(&self) -> &StagingWindowConfirmationError {
        &self.error
    }
    pub(in crate::node_runtime) fn into_publication(
        self,
    ) -> RemoteDirectIntroductionPublication<'route> {
        *self.owner
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn staging_direct_introduction_sender_terminal_owners_are_noncloneable() {
        trait NotClone<A> {
            fn check() {}
        }
        impl<T: ?Sized> NotClone<()> for T {}
        impl<T: Clone> NotClone<u8> for T {}
        let _ = <RemoteDirectIntroductionTerminal<'_> as NotClone<_>>::check;
        let _ = <RemoteDirectIntroductionTerminalFailure<'_> as NotClone<_>>::check;
        let _ = <RemoteIntroductionRetirementScope<'_, '_> as NotClone<_>>::check;
    }
}
