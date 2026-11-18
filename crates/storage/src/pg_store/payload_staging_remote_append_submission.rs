// Copyright The Argmin Authors.
// SPDX-License-Identifier: Apache-2.0

//! Only this child of the owning submission can combine the complete context
//! with its mutable installation journal. Transport never receives a resettable
//! description or a caller-supplied previous observation.
use super::*;
use crate::node_client::CompletePrimarySessionBorrow;
use crate::node_runtime::pg_store::{
    StagingWindowConfirmationError as Error, StreamAppendInstallationProof,
};
use crate::storage_rpc::{complete_applied, complete_stream_append_install as wire};
use crate::StoreError;

pub(in crate::node_runtime) struct RemoteStreamAppendInstallationAttempt<'a, 'route> {
    proof: &'a StreamAppendInstallationProof<'a, 'route>,
    bytes: Vec<u8>,
    observation: &'a mut InstallationObservation,
    previous: InstallationObservation,
}

impl ContextualSubmission {
    pub(in crate::node_runtime::pg_store) fn install_remote_append(
        &mut self,
        proof: &StreamAppendInstallationProof<'_, '_>,
        held: &mut CompletePrimarySessionBorrow<'_>,
    ) -> Result<(), Error> {
        let encoded = self
            .context
            .encode_command()
            .map_err(|e| Error::Window(e.to_string()))?;
        let preparation =
            proof.remote_request(self.context.id(), encoded.command_sha256(), held)?;
        // Confirmed installation is historical evidence. Validate the supplied
        // authority/session first, but never redispatch or erase that evidence.
        if self.installation == InstallationObservation::Installed {
            return Ok(());
        }
        let route =
            crate::storage_rpc::complete_stream_append_prepare::decode_request(preparation.bytes())
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
        // Includes panic/unwind and every ambiguous send. A definitive local
        // NotSent restores only the previous history, never a fresh state.
        self.installation = InstallationObservation::Unconfirmed;
        held.install_stream_append(RemoteStreamAppendInstallationAttempt {
            proof,
            bytes,
            observation: &mut self.installation,
            previous,
        })
    }
}

impl RemoteStreamAppendInstallationAttempt<'_, '_> {
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
