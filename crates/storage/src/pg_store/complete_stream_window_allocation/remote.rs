// Copyright The Argmin Authors.
// SPDX-License-Identifier: Apache-2.0

//! One transport attempt, borrowing consumed original custody. No reconnect,
//! retry, or caller-selected deadline; all failures retain the entire holder.
use super::*;
use crate::storage_rpc::complete_stream_allocation as wire;

pub(in crate::node_runtime) struct RemoteStreamAllocationAttempt<'a, 'route> {
    window: &'a ConfirmedStagingWindow<'route>,
    bytes: Vec<u8>,
}

impl<'route> ConfirmedStagingWindow<'route> {
    pub(super) fn stream_allocation_bytes(
        &self,
        request: &PrepareStreamUploadSegmentAppendReq,
        expected: &ExpectedStreamAppendState,
    ) -> Result<Vec<u8>, Error> {
        let node = self.terminal.owner.route.primary();
        match &self.terminal.owner.sender.source {
            PublicationSource::Inspected(evidence) => evidence
                .pending
                .as_ref()
                .expect("terminal context")
                .0
                .encode_stream_allocation(node, &self.read, request, expected),
            PublicationSource::Submitted { evidence, .. } => {
                evidence.encode_stream_allocation(node, &self.read, request, expected)
            }
        }
    }
    #[cfg(test)]
    pub(in crate::node_runtime) fn test_stream_allocation_bytes(
        &self,
        request: &PrepareStreamUploadSegmentAppendReq,
        expected: &ExpectedStreamAppendState,
    ) -> Vec<u8> {
        self.stream_allocation_bytes(request, expected).unwrap()
    }
    pub(in crate::node_runtime) fn allocate_stream_remote(
        self,
        client: &UnixStorageNodeClient,
        session: &UnixStorageNodeMetadataCommandSession,
        request: &PrepareStreamUploadSegmentAppendReq,
        expected: &ExpectedStreamAppendState,
    ) -> Result<AllocatedStreamWindow<'route>, StreamWindowAllocationFailure<'route>> {
        let result = (|| {
            self.require_valid()?;
            self.terminal.owner.require_primary(client, session)?;
            let bytes = self.stream_allocation_bytes(request, expected)?;
            session.send_stream_allocation(
                client,
                RemoteStreamAllocationAttempt {
                    window: &self,
                    bytes,
                },
            )
        })();
        match result {
            Ok(segment) => Ok(AllocatedStreamWindow {
                window: self,
                segment,
                expected: expected.clone(),
            }),
            Err(error) => Err(StreamWindowAllocationFailure {
                window: Box::new(self),
                error,
            }),
        }
    }
}

impl RemoteStreamAllocationAttempt<'_, '_> {
    pub(in crate::node_runtime) fn require_session(
        &self,
        session: &crate::node_client::CompletePrimarySessionBorrow<'_>,
    ) -> Result<(), Error> {
        self.require_valid()?;
        self.window.read.require_remote_session(session)
    }

    pub(in crate::node_runtime) fn node_id(&self) -> crate::NodeId {
        self.window.terminal.owner.route.primary()
    }
    pub(in crate::node_runtime) fn id(&self) -> MetadataCommandId {
        self.window.id()
    }
    pub(in crate::node_runtime) fn deadline(&self) -> Instant {
        self.window.read.deadline()
    }
    pub(in crate::node_runtime) fn bytes(&self) -> &[u8] {
        &self.bytes
    }
    pub(in crate::node_runtime) fn require_valid(&self) -> Result<(), Error> {
        self.window.require_valid()
    }
    pub(in crate::node_runtime) fn confirm(
        self,
        bytes: &[u8],
    ) -> Result<StreamUploadSegmentRecord, Error> {
        self.require_valid()?;
        let segment = wire::decode_response(bytes, wire::digest(&self.bytes))
            .map_err(|e| Error::Window(e.to_string()))?;
        let request =
            wire::decode_request(&self.bytes).map_err(|e| Error::Window(e.to_string()))?;
        let description =
            crate::node_runtime::pg_store::StagingWindowInspectionDescription::decode(
                request.window.descriptor,
            )?;
        let publication = self
            .window
            .terminal
            .owner
            .sender
            .source
            .request(self.node_id())
            .map_err(|e| Error::Window(e.to_string()))?;
        description
            .request(publication.topology())?
            .require_allocated_segment(&request.preparation.request, &segment)?;
        self.require_valid()?;
        Ok(segment)
    }
}
