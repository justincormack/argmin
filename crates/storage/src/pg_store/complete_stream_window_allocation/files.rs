// Copyright The Argmin Authors.
// SPDX-License-Identifier: Apache-2.0

//! File effects consume successful allocation custody, never a supplied segment.
//! Failed binding retains that custody and offers neither reset nor reallocation.
use super::*;
use crate::types::AdmittedRouteEffectFence;
pub(in crate::node_runtime) use handoff::{
    StreamAppendInstallation, StreamAppendInstallationObservation, StreamAppendInstallationProof,
    StreamAppendPreparation,
};

#[path = "handoff.rs"]
mod handoff;
#[path = "file_sender.rs"]
mod sender;
use sender::FileAttempts;
pub(in crate::node_runtime) use sender::RemoteStreamFileAttempt;

#[must_use]
pub(in crate::node_runtime) struct StreamStagingFileWriter<'route> {
    allocated: AllocatedStreamWindow<'route>,
    fence: AdmittedRouteEffectFence,
    deadline: Instant,
    acknowledgements: RecordedFileAcknowledgements,
}

#[must_use]
pub(in crate::node_runtime) struct RemoteStreamStagingFileWriter<'route> {
    writer: StreamStagingFileWriter<'route>,
    inspection: Vec<u8>,
    cutoff: crate::storage_rpc::StorageRpcOperationDeadline,
    attempts: FileAttempts,
}

#[must_use]
pub(in crate::node_runtime) struct StreamStagingFileFailure<'route> {
    allocated: Box<AllocatedStreamWindow<'route>>,
    error: Error,
}

impl<'route> AllocatedStreamWindow<'route> {
    /// Consume successful allocation custody on its original primary section.
    /// No caller-supplied segment, expected session, observation or new cutoff
    /// can mint a writer. The original primary section must acknowledge release
    /// before the writer is returned. Live publisher selection remains gated.
    pub(in crate::node_runtime) fn into_stream_file_writer_remote(
        self,
        client: &UnixStorageNodeClient,
        session: &UnixStorageNodeMetadataCommandSession,
    ) -> Result<RemoteStreamStagingFileWriter<'route>, StreamStagingFileFailure<'route>> {
        let result = (|| {
            let window = &self.window;
            window.require_valid()?;
            window.terminal.owner.require_primary(client, session)?;
            let deadline = window
                .read
                .deadline()
                .min(window.terminal.owner.work_deadline());
            let mut held = session.borrow_complete_primary(
                client,
                self.id().pg_id(),
                window.terminal.owner.route.epoch(),
                deadline,
            )?;
            // Compare under the transport mutex before constructing/sending the
            // inspection. A reconnected same-client section is not equivalent.
            window.read.require_remote_session(&held)?;
            let bytes = self.file_inspection_bytes()?;
            held.inspect_stream_file_primary(&bytes)?;
            window.require_valid()?;
            let fence = window.capture_file_fence_until(deadline)?;
            held.release_until()?;
            window.require_valid()?;
            fence.require_valid_for(window.terminal.owner.route.epoch())?;
            Ok((fence, deadline))
        })();
        self.finish_file_binding(result)
            .and_then(StreamStagingFileWriter::into_remote_dispatch)
    }

    pub(in crate::node_runtime) fn into_stream_file_dispatch_embedded(
        self,
        section: EmbeddedPublicationSection<'_>,
    ) -> Result<RemoteStreamStagingFileWriter<'route>, StreamStagingFileFailure<'route>> {
        let result = self.into_stream_file_writer_embedded(&section);
        drop(section);
        result.and_then(StreamStagingFileWriter::into_remote_dispatch)
    }

    pub(in crate::node_runtime) fn into_stream_file_writer_embedded(
        self,
        section: &EmbeddedPublicationSection<'_>,
    ) -> Result<StreamStagingFileWriter<'route>, StreamStagingFileFailure<'route>> {
        let result = (|| {
            self.window.require_valid()?;
            self.window
                .terminal
                .owner
                .route
                .require_embedded_section(section)?;
            section.require_stream_file_window(&self)?;
            Ok((
                self.window.capture_file_fence(section)?,
                self.file_deadline(section)?,
            ))
        })();
        self.finish_file_binding(result)
    }

    fn finish_file_binding(
        self,
        result: Result<(AdmittedRouteEffectFence, Instant), Error>,
    ) -> Result<StreamStagingFileWriter<'route>, StreamStagingFileFailure<'route>> {
        match result {
            Ok((fence, deadline)) => Ok(StreamStagingFileWriter {
                allocated: self,
                fence,
                deadline,
                acknowledgements: RecordedFileAcknowledgements::default(),
            }),
            Err(error) => Err(StreamStagingFileFailure {
                allocated: Box::new(self),
                error,
            }),
        }
    }

    fn file_inspection_bytes(&self) -> Result<Vec<u8>, Error> {
        let request = PrepareStreamUploadSegmentAppendReq {
            session_id: self.segment.session_id.clone(),
            segment_index: self.segment.segment_index,
            size: self.segment.size,
            segment_crc64: self.segment.segment_crc64,
            payload_crc64: self.segment.payload_crc64,
        };
        let allocation = self
            .window
            .stream_allocation_bytes(&request, &self.expected)?;
        crate::storage_rpc::complete_stream_file_primary::encode_request(&allocation, &self.segment)
            .map_err(|error| Error::Window(error.to_string()))
    }

    pub(in crate::node_runtime) fn file_deadline(
        &self,
        section: &EmbeddedPublicationSection<'_>,
    ) -> Result<Instant, Error> {
        self.window.file_deadline(section)
    }

    pub(in crate::node_runtime) fn require_file_store(
        &self,
        section: &EmbeddedPublicationSection<'_>,
        pg: &PgStore,
    ) -> Result<(), Error> {
        self.file_deadline(section)?;
        self.window.read.require_stream_files(
            pg,
            section.node_id(),
            section.runtime()?.pg_topology(),
            &self.segment,
            &self.expected,
        )
    }
}

impl<'route> StreamStagingFileWriter<'route> {
    fn into_remote_dispatch(
        self,
    ) -> Result<RemoteStreamStagingFileWriter<'route>, StreamStagingFileFailure<'route>> {
        let window = &self.allocated.window;
        let checked = window.require_valid().and_then(|()| {
            self.fence
                .require_valid_for(window.terminal.owner.route.epoch())?;
            self.allocated.file_inspection_bytes()
        });
        match checked {
            Ok(inspection) => {
                // Freeze the transport upper bound once; retries cannot project
                // a later cutoff or replace the successful allocation evidence.
                let deadline = self.fence.deadline().expect("bounded confirmed window");
                let cutoff = crate::storage_rpc::StorageRpcOperationDeadline {
                    portable_wall_valid_until_ms: deadline
                        .portable_wall_valid_until_ms()
                        .min(deadline.authority_valid_until_ms()),
                };
                Ok(RemoteStreamStagingFileWriter {
                    writer: self,
                    inspection,
                    cutoff,
                    attempts: FileAttempts::default(),
                })
            }
            Err(error) => Err(StreamStagingFileFailure {
                allocated: Box::new(self.allocated),
                error,
            }),
        }
    }
    #[cfg(test)]
    pub(in crate::node_runtime::pg_store) fn test_binding_state(
        &self,
    ) -> (Instant, Option<ContextSubmissionObservation>) {
        (
            self.deadline,
            self.allocated.window.installation_observation(),
        )
    }

    #[cfg(test)]
    pub(in crate::node_runtime) fn test_persist_acknowledgements_on_runtime(
        &self,
        runtime: &crate::node::LocalNodeRuntime,
    ) -> Result<(), Error> {
        self.acknowledgements.persist_on_runtime(
            &self.allocated.window,
            self.fence,
            self.deadline,
            runtime,
        )
    }

    pub(in crate::node_runtime) fn id(&self) -> MetadataCommandId {
        self.allocated.id()
    }

    pub(in crate::node_runtime) fn write_embedded(
        &mut self,
        shard_index: u8,
        bytes: &[u8],
    ) -> Result<crate::WriteAck, Error> {
        let window = &self.allocated.window;
        window.require_valid()?;
        let route = &window.terminal.owner.route;
        let (placement, _) = route.staging_file_placement()?;
        let (location, _) = window.read.stream_file_location(
            route.topology(),
            placement,
            &self.allocated.segment,
            shard_index,
            bytes.len(),
        )?;
        self.acknowledgements
            .bind_size(self.allocated.segment.size)?;
        let ack = route
            .embedded_staging_file_runtime(&location)?
            .write_stream_staging_file(self, shard_index, bytes)?;
        self.acknowledgements.record(shard_index, ack);
        Ok(ack)
    }

    pub(in crate::node_runtime) fn persist_acknowledgements_embedded(
        &mut self,
    ) -> Result<(), Error> {
        self.acknowledgements
            .persist_embedded(&self.allocated.window, self.fence, self.deadline)
    }

    pub(in crate::node_runtime) fn require_file_runtime(
        &self,
        runtime: &crate::node::LocalNodeRuntime,
        shard_index: u8,
        bytes: usize,
    ) -> Result<
        (
            crate::cluster::ShardLocation,
            crate::ShardKey,
            AdmittedRouteEffectFence,
        ),
        Error,
    > {
        let window = &self.allocated.window;
        window.require_valid()?;
        let route = &window.terminal.owner.route;
        let (placement, _) = route.staging_file_placement()?;
        let (location, key) = window.read.stream_file_location(
            route.topology(),
            placement,
            &self.allocated.segment,
            shard_index,
            bytes,
        )?;
        if !std::ptr::eq(runtime, route.embedded_staging_file_runtime(&location)?) {
            return Err(Error::Window("stream file runtime domain".into()));
        }
        self.fence.require_valid_for(route.epoch())?;
        Ok((location, key, self.fence))
    }
}

impl RemoteStreamStagingFileWriter<'_> {
    pub(in crate::node_runtime) fn id(&self) -> MetadataCommandId {
        self.writer.id()
    }
    #[cfg(test)]
    pub(in crate::node_runtime::pg_store) fn test_binding_state(
        &self,
    ) -> (Instant, Option<ContextSubmissionObservation>) {
        self.writer.test_binding_state()
    }
}

impl StreamStagingFileFailure<'_> {
    #[cfg(test)]
    pub(in crate::node_runtime::pg_store) fn test_binding_state(
        &self,
    ) -> (Instant, Option<ContextSubmissionObservation>) {
        (
            self.allocated.window.read.deadline(),
            self.allocated.window.installation_observation(),
        )
    }

    pub(in crate::node_runtime) fn id(&self) -> MetadataCommandId {
        self.allocated.id()
    }
    pub(in crate::node_runtime) fn error(&self) -> &Error {
        &self.error
    }
}
