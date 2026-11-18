// Copyright The Argmin Authors.
// SPDX-License-Identifier: Apache-2.0

//! Consuming, section-bound introduction publication; no detached sender or
//! physical-write/cleanup authority escapes this owner.
use super::*;
use crate::metadata_command::MetadataCommandReplicaState;
use crate::node_runtime::pg_store::{
    ApplicationObservation, CompletePublicationSender, PublicationObservation,
};

#[path = "payload_staging_direct_introduction_terminal.rs"]
mod terminal;
pub(in crate::node_runtime::pg_store) use terminal::DirectIntroductionFileScope;
pub(in crate::node_runtime::pg_store) use terminal::DirectIntroductionRetirementScope;
pub(in crate::node_runtime::pg_store) use terminal::DirectIntroductionWindowScope;

#[must_use]
pub(in crate::node_runtime) struct DirectIntroductionPublication<'section, 'route> {
    section: &'section EmbeddedPublicationSection<'route>,
    cohort: crate::cluster::CompleteFanoutRoute<'route>,
    sender: CompletePublicationSender,
    fence: AdmittedRouteEffectFence,
    deadline: Instant,
}

#[must_use]
pub(in crate::node_runtime) struct DirectIntroductionPublicationFailure<'section, 'route> {
    owner: Box<CapturedDirectIntroduction<'section, 'route>>,
    error: StagingWindowConfirmationError,
}

impl<'section, 'route> DirectIntroductionPublicationFailure<'section, 'route> {
    pub(in crate::node_runtime) fn error(&self) -> &StagingWindowConfirmationError {
        &self.error
    }
    pub(in crate::node_runtime) fn into_introduction(
        self,
    ) -> CapturedDirectIntroduction<'section, 'route> {
        *self.owner
    }
}

impl<'section, 'route> CapturedDirectIntroduction<'section, 'route> {
    /// Moving custody is effect-free, even after expiry. Every subsequent effect
    /// still requires the original section and immutable captured bounds.
    pub(in crate::node_runtime) fn into_publication(
        self,
    ) -> Result<
        DirectIntroductionPublication<'section, 'route>,
        DirectIntroductionPublicationFailure<'section, 'route>,
    > {
        if self.installation() != DirectIntroductionInstallation::Installed {
            return Err(DirectIntroductionPublicationFailure {
                owner: Box::new(self),
                error: StagingWindowConfirmationError::Window(
                    "direct introduction publication requires confirmed installation".into(),
                ),
            });
        }
        let sender = self.submission.into_publication_sender_with_cutoff(
            self.section.node_id(),
            self.publication_deadline,
            self.publication_cutoff,
        );
        Ok(DirectIntroductionPublication {
            section: self.section,
            cohort: self.cohort,
            sender,
            fence: self.fence,
            deadline: self.publication_deadline,
        })
    }
}

impl DirectIntroductionPublication<'_, '_> {
    fn require_valid(&self) -> Result<(), StagingWindowConfirmationError> {
        self.section.require_effect()?;
        self.cohort.require_embedded_section(self.section)?;
        self.fence.require_valid_for(self.section.epoch())?;
        if Instant::now() >= self.deadline {
            return Err(StoreError::OperationDeadlineExceeded {
                context: "direct introduction original publication deadline",
            }
            .into());
        }
        Ok(())
    }

    pub(in crate::node_runtime) fn apply(
        &mut self,
        node: NodeId,
    ) -> Result<MetadataCommandReplicaState, StagingWindowConfirmationError> {
        self.apply_inner(
            node,
            #[cfg(test)]
            |_| Ok(()),
        )
    }
    fn apply_inner(
        &mut self,
        node: NodeId,
        #[cfg(test)] hook: impl FnMut(&'static str) -> Result<(), StoreError>,
    ) -> Result<MetadataCommandReplicaState, StagingWindowConfirmationError> {
        self.require_valid()?;
        self.sender
            .apply_embedded_in_cohort(
                &self.cohort,
                self.section,
                node,
                self.deadline,
                #[cfg(test)]
                hook,
            )
            .map_err(|error| StagingWindowConfirmationError::Window(error.to_string()))
    }
    pub(in crate::node_runtime) fn application(&self, node: NodeId) -> ApplicationObservation {
        self.sender.application_observation(node)
    }
    pub(in crate::node_runtime) fn apply_remote_replica(
        &mut self,
        client: &crate::node_runtime::clients::UnixStorageNodeClient,
    ) -> Result<MetadataCommandReplicaState, StagingWindowConfirmationError> {
        // Keep the original primary serialization borrowed through the closed
        // remote attempt. No replacement client, section or deadline is accepted.
        self.require_valid()?;
        self.sender
            .apply_remote_replica_in_cohort(&self.cohort, client, self.deadline)
            .map_err(|error| StagingWindowConfirmationError::Window(error.to_string()))
    }
    pub(in crate::node_runtime) fn completion(
        &self,
    ) -> Result<DirectIntroductionCompletion<'_, '_, '_>, StagingWindowConfirmationError> {
        self.require_complete()?;
        Ok(DirectIntroductionCompletion { owner: self })
    }
    fn require_complete(&self) -> Result<usize, StagingWindowConfirmationError> {
        self.require_valid()?;
        self.sender
            .confirmed_cohort_members(&self.cohort)
            .map_err(|error| StagingWindowConfirmationError::Window(error.to_string()))
    }
    #[cfg(test)]
    pub(in crate::node_runtime::pg_store) fn test_apply(
        &mut self,
        node: NodeId,
        hook: impl FnMut(&'static str) -> Result<(), StoreError>,
    ) -> Result<MetadataCommandReplicaState, StagingWindowConfirmationError> {
        self.apply_inner(node, hook)
    }
    pub(in crate::node_runtime) fn id(&self) -> MetadataCommandId {
        self.sender
            .id()
            .expect("introduction retains complete submission")
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
        self.publish_inner(
            #[cfg(test)]
            |_| Ok(()),
        )
    }
    fn publish_inner(
        &mut self,
        #[cfg(test)] hook: impl FnMut(&'static str) -> Result<(), StoreError>,
    ) -> Result<(), StagingWindowConfirmationError> {
        self.require_valid()?;
        #[cfg(not(test))]
        let result = self.sender.send_embedded(self.section, self.deadline);
        #[cfg(test)]
        let result = self
            .sender
            .test_send_embedded_with_hook(self.section, self.deadline, hook);
        result.map_err(|error| StagingWindowConfirmationError::Window(error.to_string()))
    }
    #[cfg(test)]
    pub(in crate::node_runtime::pg_store) fn test_publish(
        &mut self,
        hook: impl FnMut(&'static str) -> Result<(), StoreError>,
    ) -> Result<(), StagingWindowConfirmationError> {
        self.publish_inner(hook)
    }
    #[cfg(test)]
    pub(in crate::node_runtime::pg_store) fn test_installation(
        &self,
    ) -> submission::InstallationObservation {
        self.sender
            .installation_observation()
            .expect("submitted introduction")
            .installation
    }
    #[cfg(test)]
    pub(in crate::node_runtime::pg_store) fn test_original_deadlines(
        &self,
    ) -> (Instant, StorageRpcOperationDeadline) {
        (self.deadline, self.sender.test_captured_cutoff())
    }
}

/// Borrowed all-member evidence, never independently usable terminal authority.
pub(in crate::node_runtime) struct DirectIntroductionCompletion<'owner, 'section, 'route> {
    owner: &'owner DirectIntroductionPublication<'section, 'route>,
}

impl DirectIntroductionCompletion<'_, '_, '_> {
    pub(in crate::node_runtime) fn id(&self) -> MetadataCommandId {
        self.owner.id()
    }
    pub(in crate::node_runtime) fn confirmed_members(
        &self,
    ) -> Result<usize, StagingWindowConfirmationError> {
        self.owner.require_complete()
    }
}

#[cfg(test)]
mod api {
    use super::*;
    #[test]
    fn staging_direct_introduction_publication_owners_are_noncloneable() {
        trait NotClone<A> {
            fn check() {}
        }
        impl<T: ?Sized> NotClone<()> for T {}
        impl<T: Clone> NotClone<u8> for T {}
        let _ = <DirectIntroductionPublication<'_, '_> as NotClone<_>>::check;
        let _ = <DirectIntroductionPublicationFailure<'_, '_> as NotClone<_>>::check;
        let _ = <DirectIntroductionCompletion<'_, '_, '_> as NotClone<_>>::check;
    }
}
