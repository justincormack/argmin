// Copyright The Argmin Authors.
// SPDX-License-Identifier: Apache-2.0

//! Consuming read-only handoff, retaining the original remote terminal custody.
use super::*;
use crate::node_runtime::pg_store::payload_staging::CapturedWindowRead;
use crate::node_runtime::pg_store::StagingWindowConfirmationObservation;

#[path = "payload_staging_direct_introduction_sender_files.rs"]
mod files;
pub(in crate::node_runtime::pg_store) use files::RemoteIntroductionFileScope;

#[must_use = "window confirmation retains cleanup custody; not physical-write authority"]
pub(in crate::node_runtime) struct RemoteDirectIntroductionWindow<'route> {
    terminal: RemoteDirectIntroductionTerminal<'route>,
    read: CapturedWindowRead,
}

#[must_use = "failed confirmation retains the original terminal owner"]
pub(in crate::node_runtime) struct RemoteDirectIntroductionWindowFailure<'route> {
    terminal: Box<RemoteDirectIntroductionTerminal<'route>>,
    error: StagingWindowConfirmationError,
}

/// Minted only while consuming the original terminal owner, after retirement.
pub(in crate::node_runtime::pg_store) struct RemoteIntroductionWindowScope<'a, 'route> {
    terminal: &'a RemoteDirectIntroductionTerminal<'route>,
}

impl<'a, 'route> RemoteIntroductionWindowScope<'a, 'route> {
    pub(in crate::node_runtime::pg_store) fn into_parts(
        self,
    ) -> (
        &'a CompletePublicationSender,
        &'a crate::cluster::CompleteFanoutRoute<'route>,
        &'a UnixStorageNodeClient,
        &'a UnixStorageNodeMetadataCommandSession,
        Instant,
    ) {
        let owner = &self.terminal.owner;
        (
            &owner.sender,
            owner.binding.route,
            owner.binding.client,
            owner.binding.session,
            owner.binding.deadline,
        )
    }
}

impl<'route> RemoteDirectIntroductionTerminal<'route> {
    fn require_window_confirmation(&self) -> Result<(), StagingWindowConfirmationError> {
        self.require_valid()?;
        if self.retirement != Retirement::Retired {
            return Err(StagingWindowConfirmationError::Window(
                "remote introduction window requires confirmed slot retirement".into(),
            ));
        }
        Ok(())
    }

    pub(in crate::node_runtime) fn into_confirmed_window(
        self,
    ) -> Result<RemoteDirectIntroductionWindow<'route>, RemoteDirectIntroductionWindowFailure<'route>>
    {
        self.confirm_inner(
            #[cfg(test)]
            |_| {},
        )
    }

    fn confirm_inner(
        self,
        #[cfg(test)] mut hook: impl FnMut(&'static str),
    ) -> Result<RemoteDirectIntroductionWindow<'route>, RemoteDirectIntroductionWindowFailure<'route>>
    {
        let result = (|| {
            self.require_window_confirmation()?;
            let read = CompletePublicationSender::capture_remote_direct_introduction_window(
                RemoteIntroductionWindowScope { terminal: &self },
                #[cfg(test)]
                &mut hook,
            )?;
            #[cfg(test)]
            hook("after introduction window inspection");
            // A positive remote observation cannot renew either original bound
            // or survive loss of the exact primary session before handoff.
            self.require_window_confirmation()?;
            read.require_valid()?;
            Ok(read)
        })();
        match result {
            Ok(read) => Ok(RemoteDirectIntroductionWindow {
                terminal: self,
                read,
            }),
            Err(error) => Err(RemoteDirectIntroductionWindowFailure {
                terminal: Box::new(self),
                error,
            }),
        }
    }

    #[cfg(test)]
    pub(in crate::node_runtime::pg_store) fn test_confirm_window(
        self,
        hook: impl FnMut(&'static str),
    ) -> Result<RemoteDirectIntroductionWindow<'route>, RemoteDirectIntroductionWindowFailure<'route>>
    {
        self.confirm_inner(hook)
    }
}

impl RemoteDirectIntroductionWindow<'_> {
    pub(in crate::node_runtime) fn id(&self) -> MetadataCommandId {
        self.terminal.id()
    }
    pub(in crate::node_runtime) fn observation(&self) -> &StagingWindowConfirmationObservation {
        self.read.observation()
    }
    pub(in crate::node_runtime) fn require_valid(
        &self,
    ) -> Result<(), StagingWindowConfirmationError> {
        self.terminal.require_window_confirmation()?;
        self.read.require_valid()
    }
    #[cfg(test)]
    pub(in crate::node_runtime::pg_store) fn test_bounds(&self) -> (Instant, u64) {
        self.terminal.test_bounds()
    }
}

impl<'route> RemoteDirectIntroductionWindowFailure<'route> {
    pub(in crate::node_runtime) fn error(&self) -> &StagingWindowConfirmationError {
        &self.error
    }
    pub(in crate::node_runtime) fn into_terminal(self) -> RemoteDirectIntroductionTerminal<'route> {
        *self.terminal
    }
}

#[cfg(test)]
mod api {
    use super::*;
    #[test]
    fn staging_direct_introduction_sender_window_owners_are_noncloneable() {
        trait NotClone<A> {
            fn check() {}
        }
        impl<T: ?Sized> NotClone<()> for T {}
        impl<T: Clone> NotClone<u8> for T {}
        let _ = <RemoteDirectIntroductionWindow<'_> as NotClone<_>>::check;
        let _ = <RemoteDirectIntroductionWindowFailure<'_> as NotClone<_>>::check;
        let _ = <RemoteIntroductionWindowScope<'_, '_> as NotClone<_>>::check;
    }
}
