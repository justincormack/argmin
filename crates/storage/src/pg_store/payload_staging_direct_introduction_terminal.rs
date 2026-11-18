// Copyright The Argmin Authors.
// SPDX-License-Identifier: Apache-2.0

//! One-way introduction handoff. The original section, cohort, command and
//! confirmation history remain owned together. Slot retirement uses only that
//! held section; window confirmation and physical writes remain separate gates.
use super::*;
use crate::node_runtime::pg_store::TerminalSlotRetirementObservation as Retirement;

#[path = "payload_staging_direct_introduction_window.rs"]
mod window;
pub(in crate::node_runtime::pg_store) use window::DirectIntroductionFileScope;
pub(in crate::node_runtime::pg_store) use window::DirectIntroductionWindowScope;

#[must_use = "retain confirmed introduction custody until terminal work completes"]
pub(in crate::node_runtime) struct DirectIntroductionTerminal<'section, 'route> {
    owner: DirectIntroductionPublication<'section, 'route>,
    retirement: Retirement,
}

/// Only this owner mints the borrowed retirement capability. The receiving
/// implementation cannot be invoked with a detached command or caller-owned
/// history. This scope never escapes the closed retirement call.
pub(in crate::node_runtime::pg_store) struct DirectIntroductionRetirementScope<'a, 'section, 'route>
{
    owner: &'a DirectIntroductionPublication<'section, 'route>,
    retirement: &'a mut Retirement,
}

impl<'a, 'section, 'route> DirectIntroductionRetirementScope<'a, 'section, 'route> {
    pub(in crate::node_runtime::pg_store) fn into_parts(
        self,
    ) -> (
        &'a CompletePublicationSender,
        &'a crate::cluster::CompleteFanoutRoute<'route>,
        &'section EmbeddedPublicationSection<'route>,
        Instant,
        &'a mut Retirement,
    ) {
        (
            &self.owner.sender,
            &self.owner.cohort,
            self.owner.section,
            self.owner.deadline,
            self.retirement,
        )
    }
}

#[must_use = "failed handoff retains the same publication and application history"]
pub(in crate::node_runtime) struct DirectIntroductionTerminalFailure<'section, 'route> {
    owner: Box<DirectIntroductionPublication<'section, 'route>>,
    error: StagingWindowConfirmationError,
}

impl<'section, 'route> DirectIntroductionPublication<'section, 'route> {
    pub(in crate::node_runtime) fn into_terminal(
        self,
    ) -> Result<
        DirectIntroductionTerminal<'section, 'route>,
        DirectIntroductionTerminalFailure<'section, 'route>,
    > {
        if let Err(error) = self.require_complete() {
            return Err(DirectIntroductionTerminalFailure {
                owner: Box::new(self),
                error,
            });
        }
        // No re-admission, deadline sampling, store access, or reconstructing a
        // sender from independently supplied confirmation observations.
        Ok(DirectIntroductionTerminal {
            owner: self,
            retirement: Retirement::NoAttemptObserved,
        })
    }
}

impl DirectIntroductionTerminal<'_, '_> {
    pub(in crate::node_runtime) fn retirement(&self) -> Retirement {
        self.retirement
    }

    pub(in crate::node_runtime) fn retire(&mut self) -> Result<(), StagingWindowConfirmationError> {
        self.retire_inner(
            #[cfg(test)]
            |_| Ok(()),
        )
    }

    fn retire_inner(
        &mut self,
        #[cfg(test)] hook: impl FnMut(&'static str) -> Result<(), StoreError>,
    ) -> Result<(), StagingWindowConfirmationError> {
        self.require_valid()?;
        CompletePublicationSender::retire_direct_introduction(
            DirectIntroductionRetirementScope {
                owner: &self.owner,
                retirement: &mut self.retirement,
            },
            #[cfg(test)]
            hook,
        )
    }

    #[cfg(test)]
    pub(in crate::node_runtime::pg_store) fn test_retire(
        &mut self,
        hook: impl FnMut(&'static str) -> Result<(), StoreError>,
    ) -> Result<(), StagingWindowConfirmationError> {
        self.retire_inner(hook)
    }

    pub(in crate::node_runtime) fn id(&self) -> MetadataCommandId {
        self.owner.id()
    }

    pub(in crate::node_runtime) fn require_valid(
        &self,
    ) -> Result<(), StagingWindowConfirmationError> {
        self.owner.require_complete()?;
        Ok(())
    }

    /// Historical confirmation, not renewed route authority.
    pub(in crate::node_runtime) fn application(&self, node: NodeId) -> ApplicationObservation {
        self.owner.application(node)
    }

    #[cfg(test)]
    pub(in crate::node_runtime::pg_store) fn test_original_deadlines(
        &self,
    ) -> (Instant, StorageRpcOperationDeadline) {
        self.owner.test_original_deadlines()
    }
}

impl<'section, 'route> DirectIntroductionTerminalFailure<'section, 'route> {
    pub(in crate::node_runtime) fn error(&self) -> &StagingWindowConfirmationError {
        &self.error
    }

    pub(in crate::node_runtime) fn into_publication(
        self,
    ) -> DirectIntroductionPublication<'section, 'route> {
        *self.owner
    }
}

#[cfg(test)]
mod api {
    use super::*;

    #[test]
    fn staging_direct_introduction_terminal_owners_are_noncloneable() {
        trait NotClone<A> {
            fn check() {}
        }
        impl<T: ?Sized> NotClone<()> for T {}
        impl<T: Clone> NotClone<u8> for T {}
        let _ = <DirectIntroductionTerminal<'_, '_> as NotClone<_>>::check;
        let _ = <DirectIntroductionTerminalFailure<'_, '_> as NotClone<_>>::check;
        let _ = <DirectIntroductionRetirementScope<'_, '_, '_> as NotClone<_>>::check;
    }
}
