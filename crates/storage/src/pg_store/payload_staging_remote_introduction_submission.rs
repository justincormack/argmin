// Copyright The Argmin Authors.
// SPDX-License-Identifier: Apache-2.0

//! Keep the complete context and its installation history in the shared owner,
//! so publication transfers that same history rather than reconstructing it.
use super::*;
use crate::node_client::CompletePrimarySessionBorrow;
use crate::node_runtime::pg_store::payload_staging::direct_introduction::RemoteDirectIntroductionBinding;
use crate::node_runtime::pg_store::StagingWindowConfirmationError as Error;
use crate::storage_rpc::complete_direct_introduction_install as wire;
use crate::StoreError;
use std::time::Instant;

pub(in crate::node_runtime) struct RemoteDirectIntroductionAttempt<'a, 'route> {
    binding: &'a RemoteDirectIntroductionBinding<'route>,
    bytes: Vec<u8>,
    observation: &'a mut InstallationObservation,
    previous: InstallationObservation,
}

impl ContextualSubmission {
    pub(in crate::node_runtime::pg_store) fn install_remote_introduction(
        &mut self,
        binding: &RemoteDirectIntroductionBinding<'_>,
        held: &mut CompletePrimarySessionBorrow<'_>,
    ) -> Result<(), Error> {
        binding.require_session(held)?;
        let bytes = binding.installation_request(
            self.context.id(),
            self.complete_command_sha256()
                .map_err(|e| Error::Window(e.to_string()))?,
        )?;
        if self.installation == InstallationObservation::Installed {
            return Ok(());
        }
        let previous = self.installation;
        self.installation = InstallationObservation::Unconfirmed;
        held.install_direct_introduction(RemoteDirectIntroductionAttempt {
            binding,
            bytes,
            observation: &mut self.installation,
            previous,
        })
    }
}

impl RemoteDirectIntroductionAttempt<'_, '_> {
    pub(in crate::node_runtime) fn require_session(
        &self,
        held: &CompletePrimarySessionBorrow<'_>,
    ) -> Result<(), StoreError> {
        self.binding.require_session(held)
    }
    pub(in crate::node_runtime) fn deadline(&self) -> Instant {
        self.binding.deadline()
    }
    pub(in crate::node_runtime) fn bytes(&self) -> &[u8] {
        &self.bytes
    }
    pub(in crate::node_runtime) fn not_sent(self, error: StoreError) -> Error {
        *self.observation = self.previous;
        error.into()
    }
    pub(in crate::node_runtime) fn unconfirmed(self, error: StoreError) -> Error {
        error.into()
    }
    pub(in crate::node_runtime) fn confirm(self, reply: &[u8]) -> Result<(), Error> {
        wire::require_confirmation(&self.bytes, reply).map_err(|e| Error::Window(e.to_string()))?;
        *self.observation = InstallationObservation::Installed;
        Ok(())
    }
}
