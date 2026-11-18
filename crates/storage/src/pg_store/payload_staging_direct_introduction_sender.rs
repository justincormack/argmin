// Copyright The Argmin Authors.
// SPDX-License-Identifier: Apache-2.0

//! Consume preparation into one history. No independent context, reconnect,
//! refreshed deadline or cleanup-on-drop escapes this owner.
use super::*;

#[path = "payload_staging_direct_introduction_sender_publication.rs"]
mod publication;
pub(in crate::node_runtime) use publication::RemoteDirectIntroductionPublication;
pub(in crate::node_runtime::pg_store) use publication::RemoteIntroductionRetirementScope;
pub(in crate::node_runtime::pg_store) use publication::{
    RemoteIntroductionFileScope, RemoteIntroductionWindowScope,
};

#[must_use]
pub(in crate::node_runtime) struct RemoteDirectIntroductionSubmission<'route> {
    prepared: RemoteDirectIntroductionPreparation<'route>,
}

impl<'route> RemoteDirectIntroductionPreparation<'route> {
    pub(in crate::node_runtime) fn into_submission(
        self,
    ) -> RemoteDirectIntroductionSubmission<'route> {
        RemoteDirectIntroductionSubmission { prepared: self }
    }
}

impl RemoteDirectIntroductionSubmission<'_> {
    pub(in crate::node_runtime) fn id(&self) -> MetadataCommandId {
        self.prepared.id()
    }
    pub(in crate::node_runtime) fn installation(&self) -> DirectIntroductionInstallation {
        match self.prepared.submission.observation().installation {
            submission::InstallationObservation::NoAcceptanceObserved => {
                DirectIntroductionInstallation::NoAcceptanceObserved
            }
            submission::InstallationObservation::Unconfirmed => {
                DirectIntroductionInstallation::Unconfirmed
            }
            submission::InstallationObservation::Installed => {
                DirectIntroductionInstallation::Installed
            }
        }
    }
    pub(in crate::node_runtime) fn install(
        &mut self,
    ) -> Result<(), StagingWindowConfirmationError> {
        let mut held = self.prepared.binding.borrow_primary()?;
        self.prepared
            .submission
            .install_remote_introduction(&self.prepared.binding, &mut held)
    }
    #[cfg(test)]
    pub(in crate::node_runtime::pg_store) fn test_bounds(&self) -> (Instant, u64) {
        self.prepared.test_bounds()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn staging_direct_introduction_sender_is_noncloneable() {
        trait NotClone<A> {
            fn check() {}
        }
        impl<T: ?Sized> NotClone<()> for T {}
        impl<T: Clone> NotClone<u8> for T {}
        let _ = <RemoteDirectIntroductionSubmission<'_> as NotClone<_>>::check;
        let _ =
            <crate::node_runtime::pg_store::RemoteDirectIntroductionAttempt<'_, '_> as NotClone<
                _,
            >>::check;
    }
}
