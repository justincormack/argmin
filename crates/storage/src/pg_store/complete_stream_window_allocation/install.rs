// Copyright The Argmin Authors.
// SPDX-License-Identifier: Apache-2.0

//! Installation owns preparation, original physical custody and complete retry
//! history together. Neither a failed attempt nor Drop returns a fresh builder.
use super::*;

#[path = "publish.rs"]
mod publication;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(in crate::node_runtime) enum StreamAppendInstallationObservation {
    NoAcceptanceObserved,
    Unconfirmed,
    Installed,
}

#[must_use]
pub(in crate::node_runtime) struct AcknowledgedStreamAppendSubmission<'route> {
    prepared: PreparedAcknowledgedStreamAppend<'route>,
}

impl<'route> PreparedAcknowledgedStreamAppend<'route> {
    pub(in crate::node_runtime) fn into_submission(
        self,
    ) -> AcknowledgedStreamAppendSubmission<'route> {
        AcknowledgedStreamAppendSubmission { prepared: self }
    }
}

impl AcknowledgedStreamAppendSubmission<'_> {
    pub(in crate::node_runtime) fn install_remote(
        &mut self,
        client: &UnixStorageNodeClient,
        session: &UnixStorageNodeMetadataCommandSession,
    ) -> Result<(), Error> {
        let identity = self.prepared.remote_session.as_ref().ok_or_else(|| {
            Error::Window("remote append installation requires remote preparation".into())
        })?;
        let custody = &self.prepared.custody;
        let writer = custody.writer();
        writer
            .allocated
            .window
            .terminal
            .owner
            .require_primary(client, session)?;
        let mut held = session.borrow_complete_primary(
            client,
            self.prepared.id().pg_id(),
            self.prepared.id().cluster_epoch(),
            writer.deadline,
        )?;
        held.require_identity(identity)?;
        let proof = StreamAppendInstallationProof {
            custody,
            id: self.prepared.captured.id(),
            digest: self.prepared.captured.digest()?,
            remote_session: Some(identity),
        };
        self.prepared.captured.install_remote(&proof, &mut held)
    }
    pub(in crate::node_runtime) fn id(&self) -> MetadataCommandId {
        self.prepared.id()
    }
    pub(in crate::node_runtime) fn installation(&self) -> StreamAppendInstallationObservation {
        self.prepared.captured.installation()
    }
    pub(in crate::node_runtime) fn install_embedded(
        &mut self,
        section: &EmbeddedPublicationSection<'_>,
    ) -> Result<(), Error> {
        if self.prepared.remote_session.is_some() {
            return Err(Error::Window(
                "remote prepared append requires its original remote section".into(),
            ));
        }
        let source = StreamAppendPreparation {
            custody: &self.prepared.custody,
        };
        let request = source.publication_request()?;
        let attempt = StreamAppendInstallation {
            custody: &self.prepared.custody,
            captured: &mut self.prepared.captured,
        };
        section.install_acknowledged_stream_append(&request, attempt)
    }
    #[cfg(test)]
    pub(in crate::node_runtime::pg_store) fn test_binding_state(
        &self,
    ) -> (Instant, Option<ContextSubmissionObservation>) {
        self.prepared.test_binding_state()
    }
}

/// Exclusive attempt borrowed from the owning submission, never independently
/// constructed from an envelope/descriptor or caller-supplied observation.
pub(in crate::node_runtime) struct StreamAppendInstallation<'a, 'route> {
    custody: &'a AcknowledgedStreamStagingSegment<'route>,
    captured: &'a mut CapturedStreamAppend,
}

/// Exact complete-command identity plus the retained physical authority.
/// Only the owning attempt can mint this, without lending its mutable journal.
pub(in crate::node_runtime) struct StreamAppendInstallationProof<'a, 'route> {
    custody: &'a AcknowledgedStreamStagingSegment<'route>,
    id: MetadataCommandId,
    digest: [u8; 32],
    remote_session: Option<&'a crate::node_client::MetadataCommandSessionIdentity>,
}

impl StreamAppendInstallation<'_, '_> {
    pub(in crate::node_runtime) fn deadline(&self) -> Instant {
        self.custody.writer().deadline
    }
    pub(in crate::node_runtime) fn require_section(
        &self,
        section: &EmbeddedPublicationSection<'_>,
    ) -> Result<(), Error> {
        StreamAppendPreparation {
            custody: self.custody,
        }
        .require_section(section)
    }
    pub(in crate::node_runtime) fn execute_in_store(
        self,
        pg: &PgStore,
        authority: &EmbeddedPublicationAuthority<'_>,
        request: &CompletePublicationRequest<'_>,
    ) -> Result<(), Error> {
        let proof = StreamAppendInstallationProof {
            custody: self.custody,
            id: self.captured.id(),
            digest: self.captured.digest()?,
            remote_session: None,
        };
        self.captured
            .install_in_store(&proof, pg, authority, request)
    }
}

impl StreamAppendInstallationProof<'_, '_> {
    pub(in crate::node_runtime) fn require_remote_session(
        &self,
        held: &crate::node_client::CompletePrimarySessionBorrow<'_>,
    ) -> Result<(), Error> {
        StreamAppendPreparation {
            custody: self.custody,
        }
        .require_valid()?;
        let identity = self.remote_session.ok_or_else(|| {
            Error::Window("remote append installation requires captured session identity".into())
        })?;
        held.require_identity(identity)?;
        Ok(())
    }
    pub(in crate::node_runtime) fn deadline(&self) -> Instant {
        self.custody.writer().deadline
    }
    pub(in crate::node_runtime::pg_store) fn remote_request(
        &self,
        id: MetadataCommandId,
        digest: [u8; 32],
        held: &crate::node_client::CompletePrimarySessionBorrow<'_>,
    ) -> Result<crate::storage_rpc::complete_stream_append_prepare::EncodedRequest, Error> {
        self.require_remote_session(held)?;
        if id != self.id || digest != self.digest {
            return Err(Error::Window(
                "remote append installation complete command identity".into(),
            ));
        }
        StreamAppendPreparation {
            custody: self.custody,
        }
        .remote_request()
    }
    pub(in crate::node_runtime) fn require_section(
        &self,
        section: &EmbeddedPublicationSection<'_>,
    ) -> Result<(), Error> {
        StreamAppendPreparation {
            custody: self.custody,
        }
        .require_section(section)
    }
    pub(in crate::node_runtime::pg_store) fn require_captured(
        &self,
        captured: &CapturedStreamAppend,
        request: &CompletePublicationRequest<'_>,
    ) -> Result<AdmittedRouteEffectFence, Error> {
        if captured.id() != self.id || captured.digest()? != self.digest {
            return Err(Error::Window(
                "append installation complete command identity".into(),
            ));
        }
        let source = StreamAppendPreparation {
            custody: self.custody,
        };
        let original = source.publication_request()?;
        if original.id() != request.id()
            || original.command_sha256() != request.command_sha256()
            || original.topology() != request.topology()
            || original.node_id() != request.node_id()
        {
            return Err(Error::Window(
                "append installation original source identity".into(),
            ));
        }
        Ok(self.custody.writer().fence)
    }
}
