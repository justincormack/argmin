// Copyright The Argmin Authors.
// SPDX-License-Identifier: Apache-2.0

//! Exact-session publication and ordered application. No detached generic sender,
//! retirement, physical authority or cleanup is exported from this owner.
use super::*;
use crate::node_runtime::pg_store::{CompletePublicationSender, PublicationObservation};

#[path = "payload_staging_direct_introduction_sender_application.rs"]
mod application;
pub(in crate::node_runtime::pg_store) use application::RemoteIntroductionRetirementScope;
pub(in crate::node_runtime::pg_store) use application::{
    RemoteIntroductionFileScope, RemoteIntroductionWindowScope,
};

#[must_use]
pub(in crate::node_runtime) struct RemoteDirectIntroductionPublication<'route> {
    binding: RemoteDirectIntroductionBinding<'route>,
    sender: CompletePublicationSender,
}

#[must_use]
pub(in crate::node_runtime) struct RemoteDirectIntroductionPublicationFailure<'route> {
    owner: Box<RemoteDirectIntroductionSubmission<'route>>,
    error: StagingWindowConfirmationError,
}

impl<'route> RemoteDirectIntroductionPublicationFailure<'route> {
    pub(in crate::node_runtime) fn error(&self) -> &StagingWindowConfirmationError {
        &self.error
    }
    pub(in crate::node_runtime) fn into_submission(
        self,
    ) -> RemoteDirectIntroductionSubmission<'route> {
        *self.owner
    }
}

impl<'route> RemoteDirectIntroductionSubmission<'route> {
    pub(in crate::node_runtime) fn into_publication(
        self,
    ) -> Result<
        RemoteDirectIntroductionPublication<'route>,
        RemoteDirectIntroductionPublicationFailure<'route>,
    > {
        if self.installation() != DirectIntroductionInstallation::Installed {
            return Err(RemoteDirectIntroductionPublicationFailure {
                owner: Box::new(self),
                error: StagingWindowConfirmationError::Window(
                    "remote introduction publication requires confirmed installation".into(),
                ),
            });
        }
        // Inert transfer, including after expiry: copy original bounds, never
        // sample clocks or reconstruct submission history from decoded bytes.
        let RemoteDirectIntroductionPreparation {
            binding,
            submission,
        } = self.prepared;
        let sender = submission.into_publication_sender_with_cutoff(
            binding.route.primary(),
            binding.deadline,
            binding.cutoff,
        );
        Ok(RemoteDirectIntroductionPublication { binding, sender })
    }
}

impl RemoteDirectIntroductionPublication<'_> {
    pub(in crate::node_runtime) fn id(&self) -> MetadataCommandId {
        self.sender.id().expect("retained introduction submission")
    }
    pub(in crate::node_runtime) fn installation(&self) -> DirectIntroductionInstallation {
        DirectIntroductionInstallation::Installed
    }
    pub(in crate::node_runtime) fn publication(&self) -> PublicationObservation {
        self.sender.observation()
    }
    pub(in crate::node_runtime) fn publish(
        &mut self,
    ) -> Result<(), StagingWindowConfirmationError> {
        let mut held = self.binding.borrow_primary()?;
        self.sender
            .send_remote_held(&mut held, self.binding.deadline)
            .map_err(|e| StagingWindowConfirmationError::Window(e.to_string()))
    }
    #[cfg(test)]
    pub(in crate::node_runtime::pg_store) fn test_bounds(&self) -> (Instant, u64) {
        (
            self.binding.deadline,
            self.sender
                .test_captured_cutoff()
                .portable_wall_valid_until_ms,
        )
    }
    #[cfg(test)]
    pub(in crate::node_runtime::pg_store) fn test_installation(
        &self,
    ) -> submission::InstallationObservation {
        self.sender.installation_observation().unwrap().installation
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn staging_direct_introduction_sender_publication_owners_are_noncloneable() {
        trait NotClone<A> {
            fn check() {}
        }
        impl<T: ?Sized> NotClone<()> for T {}
        impl<T: Clone> NotClone<u8> for T {}
        let _ = <RemoteDirectIntroductionPublication<'_> as NotClone<_>>::check;
        let _ = <RemoteDirectIntroductionPublicationFailure<'_> as NotClone<_>>::check;
    }
}
