// Copyright The Argmin Authors.
// SPDX-License-Identifier: Apache-2.0

//! A borrowed attempt cannot detach the command from its mutable history or
//! physical owner. The receiver's exact preparation is retained, not rebuilt.
use super::*;
use crate::node_client::CompletePrimarySessionBorrow;
use crate::node_runtime::pg_store::{
    DirectTerminalInstallationProof, StagingWindowConfirmationError as Error,
};
use crate::storage_rpc::{
    complete_applied, complete_direct_install as wire, complete_direct_prepare,
};
use crate::StoreError;

pub(in crate::node_runtime) struct RemoteDirectInstallationAttempt<'a, 'route> {
    proof: &'a DirectTerminalInstallationProof<'a, 'route>,
    bytes: Vec<u8>,
    observation: &'a mut InstallationObservation,
    previous: InstallationObservation,
}

impl ContextualSubmission {
    pub(in crate::node_runtime::pg_store) fn install_remote_direct(
        &mut self,
        proof: &DirectTerminalInstallationProof<'_, '_>,
        preparation: &complete_direct_prepare::EncodedRequest,
        held: &mut CompletePrimarySessionBorrow<'_>,
    ) -> Result<(), Error> {
        let encoded = self
            .context
            .encode_command()
            .map_err(|e| Error::Window(e.to_string()))?;
        proof.require_remote_preparation(
            self.context.id(),
            encoded.command_sha256(),
            preparation.bytes(),
            held,
        )?;
        // Validate the supplied authority even after confirmation, but never
        // redispatch or regress historical installation success.
        if self.installation == InstallationObservation::Installed {
            return Ok(());
        }
        let prepared = complete_direct_prepare::decode_request(preparation.bytes())
            .map_err(|e| Error::Window(e.to_string()))?;
        let route =
            crate::storage_rpc::complete_direct_snapshot::decode_request(prepared.snapshot_request)
                .map_err(|e| Error::Window(e.to_string()))?
                .receiver;
        let bytes = wire::encode_request(wire::Request {
            preparation: preparation.bytes(),
            command: complete_applied::BorrowedRequest {
                node_id: route.node,
                epoch: route.epoch,
                pg_id: route.pg,
                checksum: encoded.checksum_crc64(),
                command: encoded.bytes(),
            },
        })
        .map_err(|e| Error::Window(e.to_string()))?;
        let previous = self.installation;
        self.installation = InstallationObservation::Unconfirmed;
        held.install_direct_terminal(RemoteDirectInstallationAttempt {
            proof,
            bytes,
            observation: &mut self.installation,
            previous,
        })
    }
}

impl RemoteDirectInstallationAttempt<'_, '_> {
    pub(in crate::node_runtime) fn require_session(
        &self,
        held: &CompletePrimarySessionBorrow<'_>,
    ) -> Result<(), Error> {
        self.proof.require_remote_session(held)
    }
    pub(in crate::node_runtime) fn deadline(&self) -> std::time::Instant {
        self.proof.deadline()
    }
    pub(in crate::node_runtime) fn bytes(&self) -> &[u8] {
        &self.bytes
    }
    pub(in crate::node_runtime) fn not_sent(self, error: Error) -> Error {
        *self.observation = self.previous;
        error
    }
    pub(in crate::node_runtime) fn unconfirmed(self, error: StoreError) -> Error {
        Error::Store(error)
    }
    pub(in crate::node_runtime) fn confirm(self, reply: &[u8]) -> Result<(), Error> {
        wire::require_confirmation(&self.bytes, reply).map_err(|e| Error::Window(e.to_string()))?;
        *self.observation = InstallationObservation::Installed;
        Ok(())
    }
}
