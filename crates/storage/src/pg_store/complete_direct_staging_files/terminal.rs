// Copyright The Argmin Authors.
// SPDX-License-Identifier: Apache-2.0

//! Consuming command preparation. Installation is a separate owning handoff;
//! neither the command nor the physical custody can be extracted independently.
use super::*;
use crate::node_runtime::pg_store::payload_staging::CapturedDirectTerminal;

// Keep the encrypted orchestration inside the custody owner subtree so its
// consuming transitions need no externally callable raw-state constructors.
#[path = "encryption.rs"]
mod encryption;

#[path = "terminal_remote.rs"]
mod remote;

#[path = "install.rs"]
mod installation;
use installation::{
    DirectPutTerminal, DirectPutTerminalFailure, DirectTerminalPublication,
    DirectTerminalSubmission, RemoteDirectPutTerminal,
};
pub(in crate::node_runtime) use installation::{
    DirectTerminalInstallation, DirectTerminalInstallationObservation,
    DirectTerminalInstallationProof,
};

#[must_use]
pub(in crate::node_runtime) struct PreparedDirectTerminal<'route> {
    custody: WitnessedDirectPayload<'route>,
    command: CapturedDirectTerminal,
}

#[must_use]
pub(in crate::node_runtime) struct DirectTerminalPreparationFailure<'route> {
    custody: Box<WitnessedDirectPayload<'route>>,
    error: Error,
}

impl<'route> DirectTerminalPreparationFailure<'route> {
    pub(in crate::node_runtime) fn error(&self) -> &Error {
        &self.error
    }
    pub(in crate::node_runtime) fn into_witnessed(self) -> WitnessedDirectPayload<'route> {
        *self.custody
    }
}

pub(in crate::node_runtime) struct DirectTerminalPreparation<'a, 'route> {
    custody: &'a WitnessedDirectPayload<'route>,
    input: &'a crate::StreamPutCommitInput,
    committed: &'a crate::types::CommittedObjectEncryption,
}

impl<'route> WitnessedDirectPayload<'route> {
    /// Reuse the logical standard-object commit DTO, not its legacy physical
    /// request/builder. Physical identity, size, witness and proof are derived.
    pub(in crate::node_runtime) fn prepare_terminal_embedded(
        self,
        section: &EmbeddedPublicationSection<'_>,
        input: &crate::StreamPutCommitInput,
        committed: &crate::types::CommittedObjectEncryption,
    ) -> Result<PreparedDirectTerminal<'route>, DirectTerminalPreparationFailure<'route>> {
        let result = if committed.matches(
            self.prepared.custody.writer().encryption_subject(),
            &input.encryption,
        ) {
            section.prepare_direct_terminal(&DirectTerminalPreparation {
                custody: &self,
                input,
                committed,
            })
        } else {
            Err(Error::Window(
                "direct terminal committed encryption subject".into(),
            ))
        };
        match result {
            Ok(command) => Ok(PreparedDirectTerminal {
                custody: self,
                command,
            }),
            Err(error) => Err(DirectTerminalPreparationFailure {
                custody: Box::new(self),
                error,
            }),
        }
    }
}

impl PreparedDirectTerminal<'_> {
    pub(in crate::node_runtime) fn id(&self) -> MetadataCommandId {
        self.command.id()
    }
    #[cfg(test)]
    pub(in crate::node_runtime::pg_store) fn test_command(
        &self,
    ) -> crate::metadata_command::MetadataCommandEnvelope {
        self.command.test_command()
    }
    #[cfg(test)]
    pub(in crate::node_runtime::pg_store) fn test_binding_state(
        &self,
    ) -> (Instant, Option<ContextSubmissionObservation>) {
        self.custody.test_binding_state()
    }
}

impl DirectTerminalPreparation<'_, '_> {
    fn snapshot_permit(&self) -> DirectStagingPreparation<'_, '_> {
        DirectStagingPreparation {
            custody: &self.custody.prepared.custody,
        }
    }
    pub(in crate::node_runtime) fn require_section(
        &self,
        section: &EmbeddedPublicationSection<'_>,
    ) -> Result<(), Error> {
        self.snapshot_permit().require_section(section)
    }
    pub(in crate::node_runtime) fn deadline(&self) -> Instant {
        self.snapshot_permit().deadline()
    }
    pub(in crate::node_runtime) fn publication_request(
        &self,
    ) -> Result<CompletePublicationRequest<'_>, Error> {
        let owner = &self.custody.prepared.custody.writer().window.terminal.owner;
        owner
            .sender
            .source
            .request(owner.route.primary())
            .map_err(|e| Error::Window(e.to_string()))
    }
    pub(in crate::node_runtime::pg_store) fn command_in_store(
        &self,
        pg: &PgStore,
        authority: &EmbeddedPublicationAuthority<'_>,
        request: &CompletePublicationRequest<'_>,
    ) -> Result<crate::metadata_command::CommitDirectPutObjectCommand, Error> {
        self.require_authority(pg, authority, request)?;
        self.custody
            .prepared
            .custody
            .writer()
            .window
            .read
            .direct_terminal_command(
                pg,
                request,
                &self.custody.manifest,
                self.input,
                &self.custody.prepared.captured.snapshot,
            )
    }
    pub(in crate::node_runtime::pg_store) fn require_authority(
        &self,
        pg: &PgStore,
        authority: &EmbeddedPublicationAuthority<'_>,
        request: &CompletePublicationRequest<'_>,
    ) -> Result<(), Error> {
        self.snapshot_permit().require_valid()?;
        authority.require_direct_terminal(self)?;
        authority.require_effect(pg, request)?;
        let original = self.publication_request()?;
        if original.id() != request.id() || original.command_sha256() != request.command_sha256() {
            return Err(Error::Window("direct terminal original command".into()));
        }
        Ok(())
    }
    pub(in crate::node_runtime) fn capture_in_store(
        &self,
        pg: &PgStore,
        authority: &EmbeddedPublicationAuthority<'_>,
        request: &CompletePublicationRequest<'_>,
    ) -> Result<CapturedDirectTerminal, Error> {
        CapturedDirectTerminal::capture(self, pg, authority, request)
    }
}
