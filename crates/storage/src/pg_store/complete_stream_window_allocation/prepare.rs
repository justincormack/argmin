// Copyright The Argmin Authors.
// SPDX-License-Identifier: Apache-2.0

//! Consuming append preparation. Neither the physical holder nor the new
//! complete submission can escape independently. No installation occurs here.
use super::*;
use crate::node_runtime::pg_store::payload_staging::CapturedStreamAppend;

#[path = "install.rs"]
mod installation;
pub(in crate::node_runtime) use installation::{
    StreamAppendInstallation, StreamAppendInstallationObservation, StreamAppendInstallationProof,
};

#[must_use]
pub(in crate::node_runtime) struct PreparedAcknowledgedStreamAppend<'route> {
    custody: AcknowledgedStreamStagingSegment<'route>,
    captured: CapturedStreamAppend,
    // The physical phase released its first section. Remote preparation is
    // bound to this newly acquired section, for the later installation handoff.
    remote_session: Option<crate::node_client::MetadataCommandSessionIdentity>,
}

#[must_use]
pub(in crate::node_runtime) struct StreamAppendPreparationFailure<'route> {
    custody: Box<AcknowledgedStreamStagingSegment<'route>>,
    error: Error,
}

impl<'route> StreamAppendPreparationFailure<'route> {
    pub(in crate::node_runtime) fn error(&self) -> &Error {
        &self.error
    }
    pub(in crate::node_runtime) fn into_acknowledged(
        self,
    ) -> AcknowledgedStreamStagingSegment<'route> {
        *self.custody
    }
}

/// Borrowed only by the admitted node bridge while the consuming caller owns
/// the acknowledged holder. No raw segment, receipt batch or fence is accepted.
pub(in crate::node_runtime) struct StreamAppendPreparation<'a, 'route> {
    custody: &'a AcknowledgedStreamStagingSegment<'route>,
}

impl<'route> AcknowledgedStreamStagingSegment<'route> {
    pub(in crate::node_runtime) fn open_append_primary_session(
        &self,
        client: &UnixStorageNodeClient,
    ) -> Result<UnixStorageNodeMetadataCommandSession, Error> {
        StreamAppendPreparation { custody: self }.require_valid()?;
        let writer = self.writer();
        Ok(writer
            .allocated
            .window
            .terminal
            .owner
            .open_primary_session(client, writer.deadline)?)
    }

    pub(in crate::node_runtime) fn prepare_append_remote(
        self,
        client: &UnixStorageNodeClient,
        session: &UnixStorageNodeMetadataCommandSession,
    ) -> Result<PreparedAcknowledgedStreamAppend<'route>, StreamAppendPreparationFailure<'route>>
    {
        let result = (|| {
            let permit = StreamAppendPreparation { custody: &self };
            permit.require_valid()?;
            let writer = self.writer();
            let owner = &writer.allocated.window.terminal.owner;
            owner.require_primary(client, session)?;
            let mut held = session.borrow_complete_primary(
                client,
                self.id().pg_id(),
                self.id().cluster_epoch(),
                writer.deadline,
            )?;
            held.prepare_stream_append(&permit)
        })();
        match result {
            Ok(captured) => Ok(PreparedAcknowledgedStreamAppend {
                custody: self,
                captured,
                remote_session: Some(session.identity()),
            }),
            Err(error) => Err(StreamAppendPreparationFailure {
                custody: Box::new(self),
                error,
            }),
        }
    }

    pub(in crate::node_runtime) fn prepare_append_embedded(
        self,
        section: &EmbeddedPublicationSection<'_>,
    ) -> Result<PreparedAcknowledgedStreamAppend<'route>, StreamAppendPreparationFailure<'route>>
    {
        let result =
            section.prepare_acknowledged_stream_append(&StreamAppendPreparation { custody: &self });
        match result {
            Ok(captured) => Ok(PreparedAcknowledgedStreamAppend {
                custody: self,
                captured,
                remote_session: None,
            }),
            Err(error) => Err(StreamAppendPreparationFailure {
                custody: Box::new(self),
                error,
            }),
        }
    }
}

impl PreparedAcknowledgedStreamAppend<'_> {
    pub(in crate::node_runtime) fn id(&self) -> MetadataCommandId {
        self.captured.id()
    }
    #[cfg(test)]
    pub(in crate::node_runtime::pg_store) fn test_binding_state(
        &self,
    ) -> (Instant, Option<ContextSubmissionObservation>) {
        self.custody.test_binding_state()
    }
    #[cfg(test)]
    pub(in crate::node_runtime::pg_store) fn test_command(
        &self,
    ) -> crate::metadata_command::MetadataCommandEnvelope {
        self.captured.test_command()
    }
}

impl StreamAppendPreparation<'_, '_> {
    #[cfg(test)]
    pub(in crate::node_runtime::pg_store) fn test_command(
        &self,
    ) -> Result<crate::metadata_command::AppendStreamSegmentCommand, Error> {
        self.command()
    }

    pub(in crate::node_runtime) fn require_valid(&self) -> Result<(), Error> {
        let writer = self.custody.writer();
        writer.allocated.window.require_valid()?;
        writer
            .fence
            .require_valid_for(writer.allocated.window.id().cluster_epoch())?;
        if Instant::now() >= writer.deadline {
            return Err(StoreError::OperationDeadlineExceeded {
                context: "acknowledged stream append preparation",
            }
            .into());
        }
        writer.acknowledgements.require_confirmed()
    }

    pub(in crate::node_runtime) fn remote_request(
        &self,
    ) -> Result<crate::storage_rpc::complete_stream_append_prepare::EncodedRequest, Error> {
        self.require_valid()?;
        let Custody::Remote(remote) = &self.custody.custody else {
            return Err(Error::Window(
                "remote append requires retained remote physical custody".into(),
            ));
        };
        let writer = &remote.writer;
        let owner = &writer.allocated.window.terminal.owner;
        let request = crate::storage_rpc::complete_staging_ack::Request {
            receiver: crate::storage_rpc::complete_settled_prefix::Request {
                node: owner.route.primary(),
                pg: self.custody.id().pg_id(),
                epoch: owner.route.epoch(),
                topology: crate::storage_rpc::complete_settled_prefix::topology_digest(
                    owner.route.topology(),
                ),
                deadline: remote.cutoff,
            },
            provenance: crate::storage_rpc::complete_staging_ack::Provenance::Stream {
                inspection: &remote.inspection,
            },
            receipts: writer.acknowledgements.confirmed_receipts()?,
        };
        crate::storage_rpc::complete_stream_append_prepare::encode_request(&request)
            .map_err(|e| Error::Window(e.to_string()))
    }

    pub(in crate::node_runtime::pg_store) fn require_remote_context(
        &self,
        context: &crate::node_runtime::pg_store::payload_staging::ContextualCommand,
    ) -> Result<(), Error> {
        self.require_valid()?;
        let Custody::Remote(remote) = &self.custody.custody else {
            return Err(Error::Window("remote append custody".into()));
        };
        let writer = &remote.writer;
        let route = &writer.allocated.window.terminal.owner.route;
        let (placement, _) = route.staging_file_placement()?;
        writer.allocated.window.read.require_append_response(
            context,
            &self.command()?,
            route.topology(),
            placement,
            remote.cutoff.portable_wall_valid_until_ms,
        )
    }

    pub(in crate::node_runtime) fn require_section(
        &self,
        section: &EmbeddedPublicationSection<'_>,
    ) -> Result<(), Error> {
        self.require_valid()?;
        let writer = self.custody.writer();
        writer
            .allocated
            .window
            .terminal
            .owner
            .route
            .require_embedded_section(section)?;
        Ok(())
    }
    pub(in crate::node_runtime) fn deadline(&self) -> Instant {
        self.custody.writer().deadline
    }
    pub(in crate::node_runtime) fn publication_request(
        &self,
    ) -> Result<CompletePublicationRequest<'_>, Error> {
        let owner = &self.custody.writer().allocated.window.terminal.owner;
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
    ) -> Result<
        (
            crate::metadata_command::AppendStreamSegmentCommand,
            AdmittedRouteEffectFence,
        ),
        Error,
    > {
        authority.require_stream_append_preparation(self)?;
        authority.require_effect(pg, request)?;
        let original = self.publication_request()?;
        if original.id() != request.id() || original.command_sha256() != request.command_sha256() {
            return Err(Error::Window(
                "stream append preparation original command".into(),
            ));
        }
        let writer = self.custody.writer();
        let allocated = &writer.allocated;
        allocated.window.read.require_stream_files(
            pg,
            request.node_id(),
            request.topology(),
            &allocated.segment,
            &allocated.expected,
        )?;
        Ok((self.command()?, writer.fence))
    }

    fn command(&self) -> Result<crate::metadata_command::AppendStreamSegmentCommand, Error> {
        self.require_valid()?;
        let writer = self.custody.writer();
        let allocated = &writer.allocated;
        let session = match &allocated.expected {
            ExpectedStreamAppendState::PutObject { session, .. }
            | ExpectedStreamAppendState::UploadPart { session, .. } => session,
        };
        let mut segment = allocated.segment.clone();
        segment.shard_integrity = Some(
            writer
                .acknowledgements
                .stream_integrity(
                    &allocated.window,
                    writer.fence,
                    writer.deadline,
                    &segment,
                    &session.encryption,
                )?
                .durable_record(),
        );
        Ok(crate::metadata_command::AppendStreamSegmentCommand {
            bucket: session.bucket.clone(),
            key: session.key.clone(),
            target: session.target.clone(),
            segment,
        })
    }
    pub(in crate::node_runtime) fn prepare_in_store(
        &self,
        pg: &PgStore,
        authority: &EmbeddedPublicationAuthority<'_>,
        request: &CompletePublicationRequest<'_>,
        placement: PayloadPlacementInput<'_>,
    ) -> Result<CapturedStreamAppend, Error> {
        crate::node_runtime::pg_store::payload_staging::capture_acknowledged_append(
            self, pg, authority, request, placement,
        )
    }
}
