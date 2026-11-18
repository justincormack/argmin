// Copyright The Argmin Authors.
// SPDX-License-Identifier: Apache-2.0

//! All attempts retain the exact primary session through classified confirmation.
//! Completion borrows custody; it cannot retire a slot or mint physical authority.
use super::*;
use crate::metadata_command::MetadataCommandReplicaState;
use crate::node_runtime::pg_store::ApplicationObservation;

#[path = "payload_staging_direct_introduction_sender_terminal.rs"]
mod terminal;
pub(in crate::node_runtime::pg_store) use terminal::RemoteIntroductionRetirementScope;
pub(in crate::node_runtime::pg_store) use terminal::{
    RemoteIntroductionFileScope, RemoteIntroductionWindowScope,
};

pub(in crate::node_runtime) struct RemoteDirectIntroductionCompletion<'owner, 'route> {
    owner: &'owner RemoteDirectIntroductionPublication<'route>,
}

impl RemoteDirectIntroductionPublication<'_> {
    pub(in crate::node_runtime) fn application(&self, node: NodeId) -> ApplicationObservation {
        self.sender.application_observation(node)
    }

    pub(in crate::node_runtime) fn apply_remote(
        &mut self,
        target: &UnixStorageNodeClient,
    ) -> Result<MetadataCommandReplicaState, StagingWindowConfirmationError> {
        let binding = &self.binding;
        let mut held = binding.borrow_primary()?;
        binding.route.require_client(target)?;
        self.sender
            .require_cohort_application(binding.route, target.node_id())
            .map_err(|e| StagingWindowConfirmationError::Window(e.to_string()))?;
        if target.node_id() == binding.route.primary() {
            binding.session.require_origin(target)?;
            self.sender
                .apply_remote_held(&mut held, binding.deadline)
                .map_err(|e| StagingWindowConfirmationError::Window(e.to_string()))
        } else {
            self.sender
                .apply_remote_replica_in_cohort(binding.route, target, binding.deadline)
                .map_err(|e| StagingWindowConfirmationError::Window(e.to_string()))
        }
    }

    pub(in crate::node_runtime) fn apply_embedded_replica(
        &mut self,
        node: NodeId,
    ) -> Result<MetadataCommandReplicaState, StagingWindowConfirmationError> {
        self.apply_embedded_inner(
            node,
            #[cfg(test)]
            |_| Ok(()),
        )
    }

    fn apply_embedded_inner(
        &mut self,
        node: NodeId,
        #[cfg(test)] hook: impl FnMut(&'static str) -> Result<(), StoreError>,
    ) -> Result<MetadataCommandReplicaState, StagingWindowConfirmationError> {
        let binding = &self.binding;
        binding.require_authority()?;
        self.sender
            .require_cohort_application(binding.route, node)
            .map_err(|e| StagingWindowConfirmationError::Window(e.to_string()))?;
        let target = binding.route.embedded_replica_target_in_session(
            binding.client,
            binding.session,
            &binding.identity,
            node,
            binding.deadline,
        )?;
        // The target holds the original primary borrow throughout local PG
        // admission, transaction commit and outcome classification.
        binding.require_authority()?;
        #[cfg(not(test))]
        let result = self.sender.apply_embedded(&target, binding.deadline);
        #[cfg(test)]
        let result = self
            .sender
            .test_apply_embedded_with_hook(&target, binding.deadline, hook);
        result.map_err(|e| StagingWindowConfirmationError::Window(e.to_string()))
    }

    #[cfg(test)]
    pub(in crate::node_runtime::pg_store) fn test_apply_embedded_replica(
        &mut self,
        node: NodeId,
        hook: impl FnMut(&'static str) -> Result<(), StoreError>,
    ) -> Result<MetadataCommandReplicaState, StagingWindowConfirmationError> {
        self.apply_embedded_inner(node, hook)
    }

    pub(in crate::node_runtime) fn completion(
        &self,
    ) -> Result<RemoteDirectIntroductionCompletion<'_, '_>, StagingWindowConfirmationError> {
        self.require_complete()?;
        Ok(RemoteDirectIntroductionCompletion { owner: self })
    }

    fn require_complete(&self) -> Result<usize, StagingWindowConfirmationError> {
        let _held = self.binding.borrow_primary()?;
        self.sender
            .confirmed_cohort_members(self.binding.route)
            .map_err(|e| StagingWindowConfirmationError::Window(e.to_string()))
    }
}

impl RemoteDirectIntroductionCompletion<'_, '_> {
    pub(in crate::node_runtime) fn id(&self) -> MetadataCommandId {
        self.owner.id()
    }
    pub(in crate::node_runtime) fn confirmed_peers(&self) -> usize {
        self.owner.binding.route.nodes().count()
    }
    pub(in crate::node_runtime) fn require_valid(
        &self,
    ) -> Result<(), StagingWindowConfirmationError> {
        self.owner.require_complete().map(|_| ())
    }
}
