// Copyright The Argmin Authors.
// SPDX-License-Identifier: Apache-2.0

//! Consuming confirmation on the original section. No standalone window, sender,
//! descriptor, or refreshed deadline escapes the owning handoff.
use super::*;
use crate::node_runtime::pg_store::payload_staging::CapturedWindowRead;
use crate::node_runtime::pg_store::StagingWindowConfirmationObservation;

/// Consuming bridge. Only a confirmed introduction can create this owner; raw
/// sender, read, section or observations cannot be supplied independently.
pub(in crate::node_runtime::pg_store) struct DirectIntroductionFileScope<'section, 'route>(
    DirectIntroductionWindow<'section, 'route>,
);

impl<'section, 'route> DirectIntroductionFileScope<'section, 'route> {
    pub(in crate::node_runtime::pg_store) fn into_parts(
        self,
    ) -> (
        crate::cluster::CompleteFanoutRoute<'route>,
        CompletePublicationSender,
        CapturedWindowRead,
        &'section EmbeddedPublicationSection<'route>,
        Result<(), StagingWindowConfirmationError>,
    ) {
        let checked = self.0.require_valid();
        let DirectIntroductionWindow { terminal, read } = self.0;
        let owner = terminal.owner;
        (owner.cohort, owner.sender, read, owner.section, checked)
    }
}

impl<'route> DirectIntroductionWindow<'_, 'route> {
    pub(in crate::node_runtime) fn prepare_file_dispatch(
        self,
        provisional: &crate::ObjectEncryption,
    ) -> Result<
        crate::node_runtime::pg_store::DirectIntroductionFilePreparation<'route>,
        crate::node_runtime::pg_store::DirectStagingFileFailure<'route>,
    > {
        CompletePublicationSender::prepare_introduction_files(
            DirectIntroductionFileScope(self),
            provisional,
        )
    }
}

#[must_use = "confirmed window retains terminal custody; not physical-write authority"]
pub(in crate::node_runtime) struct DirectIntroductionWindow<'section, 'route> {
    terminal: DirectIntroductionTerminal<'section, 'route>,
    read: CapturedWindowRead,
}

#[must_use = "failed inspection retains the original terminal owner"]
pub(in crate::node_runtime) struct DirectIntroductionWindowFailure<'section, 'route> {
    terminal: Box<DirectIntroductionTerminal<'section, 'route>>,
    error: StagingWindowConfirmationError,
}

/// Only the consuming terminal operation can mint this short-lived borrow.
/// The closed source-reading bridge never receives independently chosen inputs.
pub(in crate::node_runtime::pg_store) struct DirectIntroductionWindowScope<'a, 'section, 'route> {
    terminal: &'a DirectIntroductionTerminal<'section, 'route>,
}

impl<'a, 'section, 'route> DirectIntroductionWindowScope<'a, 'section, 'route> {
    pub(in crate::node_runtime::pg_store) fn into_parts(
        self,
    ) -> (
        &'a CompletePublicationSender,
        &'section EmbeddedPublicationSection<'route>,
        Instant,
    ) {
        let owner = &self.terminal.owner;
        (&owner.sender, owner.section, owner.deadline)
    }
}

impl<'section, 'route> DirectIntroductionTerminal<'section, 'route> {
    fn require_window_confirmation(&self) -> Result<(), StagingWindowConfirmationError> {
        self.require_valid()?;
        if self.retirement != Retirement::Retired {
            return Err(StagingWindowConfirmationError::Window(
                "introduction window requires confirmed slot retirement".into(),
            ));
        }
        Ok(())
    }

    pub(in crate::node_runtime) fn into_confirmed_window(
        self,
    ) -> Result<
        DirectIntroductionWindow<'section, 'route>,
        DirectIntroductionWindowFailure<'section, 'route>,
    > {
        self.confirm_inner(
            #[cfg(test)]
            |_| {},
        )
    }

    fn confirm_inner(
        self,
        #[cfg(test)] mut hook: impl FnMut(&'static str),
    ) -> Result<
        DirectIntroductionWindow<'section, 'route>,
        DirectIntroductionWindowFailure<'section, 'route>,
    > {
        let result = (|| {
            self.require_window_confirmation()?;
            let read = CompletePublicationSender::capture_direct_introduction_window(
                DirectIntroductionWindowScope { terminal: &self },
                #[cfg(test)]
                &mut hook,
            )?;
            #[cfg(test)]
            hook("after introduction window inspection");
            // Even a positive inspection cannot mint custody after its authority
            // expires. Recheck the original owner as well as the captured read.
            self.require_window_confirmation()?;
            read.require_valid()?;
            Ok(read)
        })();
        match result {
            Ok(read) => Ok(DirectIntroductionWindow {
                terminal: self,
                read,
            }),
            Err(error) => Err(DirectIntroductionWindowFailure {
                terminal: Box::new(self),
                error,
            }),
        }
    }

    #[cfg(test)]
    pub(in crate::node_runtime::pg_store) fn test_confirm_window(
        self,
        hook: impl FnMut(&'static str),
    ) -> Result<
        DirectIntroductionWindow<'section, 'route>,
        DirectIntroductionWindowFailure<'section, 'route>,
    > {
        self.confirm_inner(hook)
    }
}

impl DirectIntroductionWindow<'_, '_> {
    pub(in crate::node_runtime) fn id(&self) -> MetadataCommandId {
        self.terminal.id()
    }

    pub(in crate::node_runtime) fn observation(&self) -> &StagingWindowConfirmationObservation {
        self.read.observation()
    }

    /// Checks retained admission/time bounds, not a fresh durable observation or
    /// physical-write exclusion. The owning physical handoff remains separate.
    pub(in crate::node_runtime) fn require_valid(
        &self,
    ) -> Result<(), StagingWindowConfirmationError> {
        self.terminal.require_window_confirmation()?;
        self.read.require_valid()
    }

    #[cfg(test)]
    pub(in crate::node_runtime::pg_store) fn test_original_deadlines(
        &self,
    ) -> (Instant, StorageRpcOperationDeadline) {
        self.terminal.test_original_deadlines()
    }
}

impl<'section, 'route> DirectIntroductionWindowFailure<'section, 'route> {
    pub(in crate::node_runtime) fn error(&self) -> &StagingWindowConfirmationError {
        &self.error
    }

    pub(in crate::node_runtime) fn into_terminal(
        self,
    ) -> DirectIntroductionTerminal<'section, 'route> {
        *self.terminal
    }
}

#[cfg(test)]
mod api {
    use super::*;
    #[test]
    fn staging_direct_introduction_window_owners_are_noncloneable() {
        trait NotClone<A> {
            fn check() {}
        }
        impl<T: ?Sized> NotClone<()> for T {}
        impl<T: Clone> NotClone<u8> for T {}
        let _ = <DirectIntroductionWindow<'_, '_> as NotClone<_>>::check;
        let _ = <DirectIntroductionWindowFailure<'_, '_> as NotClone<_>>::check;
        let _ = <DirectIntroductionWindowScope<'_, '_, '_> as NotClone<_>>::check;
    }
}
