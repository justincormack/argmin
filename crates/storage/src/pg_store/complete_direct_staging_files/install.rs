// Copyright The Argmin Authors.
// SPDX-License-Identifier: Apache-2.0

//! One owner for the complete command, physical custody and every install attempt.
use super::*;
use crate::types::AdmittedRouteEffectFence;

#[path = "publish.rs"]
mod publication;
pub(super) use publication::{
    DirectPutTerminal, DirectPutTerminalFailure, DirectTerminalPublication, RemoteDirectPutTerminal,
};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(in crate::node_runtime) enum DirectTerminalInstallationObservation {
    NoAcceptanceObserved,
    Unconfirmed,
    Installed,
}

#[must_use]
pub(in crate::node_runtime) struct DirectTerminalSubmission<'route> {
    prepared: PreparedDirectTerminal<'route>,
}

impl<'route> PreparedDirectTerminal<'route> {
    pub(in crate::node_runtime) fn into_submission(self) -> DirectTerminalSubmission<'route> {
        DirectTerminalSubmission { prepared: self }
    }
}

impl<'route> DirectTerminalSubmission<'route> {
    // Only the owning terminal subtree may discard an unaccepted command.
    // An uncertain earlier attempt is never undone by a later rejection.
    pub(super) fn into_unaccepted_payload(
        self,
    ) -> Result<WitnessedDirectPayload<'route>, Box<Self>> {
        if self.installation() != DirectTerminalInstallationObservation::NoAcceptanceObserved {
            return Err(Box::new(self));
        }
        Ok(self.prepared.custody)
    }
}

impl DirectTerminalSubmission<'_> {
    pub(in crate::node_runtime) fn install_remote(
        &mut self,
        client: &UnixStorageNodeClient,
        session: &UnixStorageNodeMetadataCommandSession,
    ) -> Result<(), Error> {
        let preparation = self.prepared.command.remote_preparation_digest()?;
        let writer = self.prepared.custody.prepared.custody.writer();
        writer
            .window
            .terminal
            .owner
            .require_primary(client, session)?;
        let mut held = session.borrow_complete_primary(
            client,
            self.id().pg_id(),
            self.id().cluster_epoch(),
            writer.deadline,
        )?;
        let proof = DirectTerminalInstallationProof {
            custody: &self.prepared.custody,
            id: self.prepared.command.id(),
            digest: self.prepared.command.digest()?,
            remote_preparation: Some(preparation),
        };
        self.prepared.command.install_remote(&proof, &mut held)
    }
    pub(in crate::node_runtime) fn id(&self) -> MetadataCommandId {
        self.prepared.id()
    }
    pub(in crate::node_runtime) fn installation(&self) -> DirectTerminalInstallationObservation {
        self.prepared.command.installation()
    }
    pub(in crate::node_runtime) fn install_embedded(
        &mut self,
        section: &EmbeddedPublicationSection<'_>,
    ) -> Result<(), Error> {
        let source = DirectStagingPreparation {
            custody: &self.prepared.custody.prepared.custody,
        };
        let request = source.publication_request()?;
        section.install_direct_terminal(
            &request,
            DirectTerminalInstallation {
                custody: &self.prepared.custody,
                captured: &mut self.prepared.command,
            },
        )
    }
    #[cfg(test)]
    pub(in crate::node_runtime::pg_store) fn test_binding_state(
        &self,
    ) -> (Instant, Option<ContextSubmissionObservation>) {
        self.prepared.test_binding_state()
    }
}

pub(in crate::node_runtime) struct DirectTerminalInstallation<'a, 'route> {
    custody: &'a WitnessedDirectPayload<'route>,
    captured: &'a mut CapturedDirectTerminal,
}

pub(in crate::node_runtime) struct DirectTerminalInstallationProof<'a, 'route> {
    custody: &'a WitnessedDirectPayload<'route>,
    id: MetadataCommandId,
    digest: [u8; 32],
    remote_preparation: Option<[u8; 32]>,
}

impl DirectTerminalInstallation<'_, '_> {
    pub(in crate::node_runtime) fn deadline(&self) -> Instant {
        self.custody.prepared.custody.writer().deadline
    }
    pub(in crate::node_runtime) fn require_section(
        &self,
        section: &EmbeddedPublicationSection<'_>,
    ) -> Result<(), Error> {
        DirectStagingPreparation {
            custody: &self.custody.prepared.custody,
        }
        .require_section(section)
    }
    pub(in crate::node_runtime) fn execute_in_store(
        self,
        pg: &PgStore,
        authority: &EmbeddedPublicationAuthority<'_>,
        request: &CompletePublicationRequest<'_>,
    ) -> Result<(), Error> {
        let proof = DirectTerminalInstallationProof {
            custody: self.custody,
            id: self.captured.id(),
            digest: self.captured.digest()?,
            remote_preparation: None,
        };
        self.captured
            .install_in_store(&proof, pg, authority, request)
    }
}

impl DirectTerminalInstallationProof<'_, '_> {
    pub(in crate::node_runtime) fn deadline(&self) -> Instant {
        self.custody.prepared.custody.writer().deadline
    }
    pub(in crate::node_runtime) fn require_remote_session(
        &self,
        held: &crate::node_client::CompletePrimarySessionBorrow<'_>,
    ) -> Result<(), Error> {
        if self.remote_preparation.is_none() {
            return Err(Error::Window(
                "direct installation requires remote preparation".into(),
            ));
        }
        DirectStagingPreparation {
            custody: &self.custody.prepared.custody,
        }
        .require_remote_session(held)
    }
    pub(in crate::node_runtime::pg_store) fn require_remote_preparation(
        &self,
        id: MetadataCommandId,
        digest: [u8; 32],
        preparation: &[u8],
        held: &crate::node_client::CompletePrimarySessionBorrow<'_>,
    ) -> Result<(), Error> {
        self.require_remote_session(held)?;
        self.require_identity(id, digest)?;
        if self.remote_preparation
            != Some(crate::storage_rpc::complete_stream_allocation::digest(
                preparation,
            ))
        {
            return Err(Error::Window(
                "direct installation exact preparation identity".into(),
            ));
        }
        Ok(())
    }
    pub(in crate::node_runtime) fn require_section(
        &self,
        section: &EmbeddedPublicationSection<'_>,
    ) -> Result<(), Error> {
        DirectStagingPreparation {
            custody: &self.custody.prepared.custody,
        }
        .require_section(section)
    }
    pub(in crate::node_runtime::pg_store) fn require_captured(
        &self,
        captured: &CapturedDirectTerminal,
        request: &CompletePublicationRequest<'_>,
    ) -> Result<AdmittedRouteEffectFence, Error> {
        self.require_identity(captured.id(), captured.digest()?)?;
        let preparation = DirectStagingPreparation {
            custody: &self.custody.prepared.custody,
        };
        let original = preparation.publication_request()?;
        if original.id() != request.id()
            || original.command_sha256() != request.command_sha256()
            || original.node_id() != request.node_id()
            || original.topology() != request.topology()
        {
            return Err(Error::Window(
                "direct installation original source identity".into(),
            ));
        }
        Ok(self.custody.prepared.custody.writer().fence)
    }
    pub(in crate::node_runtime::pg_store) fn require_identity(
        &self,
        id: MetadataCommandId,
        digest: [u8; 32],
    ) -> Result<(), Error> {
        if self.id != id || self.digest != digest {
            return Err(Error::Window(
                "direct installation complete command identity".into(),
            ));
        }
        Ok(())
    }
    pub(in crate::node_runtime::pg_store) fn require_snapshot(
        &self,
        pg: &PgStore,
        node: crate::NodeId,
        topology: &crate::PgTopology,
    ) -> Result<(), Error> {
        DirectStagingPreparation {
            custody: &self.custody.prepared.custody,
        }
        .require_valid()?;
        self.custody
            .prepared
            .custody
            .writer()
            .window
            .read
            .require_direct_terminal_snapshot(
                pg,
                node,
                topology,
                &self.custody.prepared.captured.snapshot,
            )
    }
}
