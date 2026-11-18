// Copyright The Argmin Authors.
// SPDX-License-Identifier: Apache-2.0

//! Consuming embedded stream allocation, not a physical-write grant. Even an
//! uncertain result keeps the original command/window; no retry or extraction
//! can turn it back into fresh allocation authority.

use super::*;
use crate::node_runtime::engine::EmbeddedPublicationAuthority;
use crate::node_runtime::payload_placement::PayloadPlacementInput;
use crate::node_runtime::pg_store::{CompletePublicationRequest, PgStore};
use crate::types::ExpectedStreamAppendState;
use crate::{PrepareStreamUploadSegmentAppendReq, StreamUploadSegmentRecord};
pub(in crate::node_runtime) use files::{
    StreamAppendInstallation, StreamAppendInstallationObservation, StreamAppendInstallationProof,
    StreamAppendPreparation,
};

#[path = "complete_stream_window_allocation/remote.rs"]
mod remote;
pub(in crate::node_runtime) use remote::RemoteStreamAllocationAttempt;

#[path = "complete_stream_window_allocation/files.rs"]
mod files;
#[cfg(test)]
pub(in crate::node_runtime) use files::RemoteStreamStagingFileWriter;
pub(in crate::node_runtime) use files::{RemoteStreamFileAttempt, StreamStagingFileWriter};

#[must_use]
pub(in crate::node_runtime) struct AllocatedStreamWindow<'route> {
    window: ConfirmedStagingWindow<'route>,
    segment: StreamUploadSegmentRecord,
    expected: ExpectedStreamAppendState,
}

#[must_use]
pub(in crate::node_runtime) struct StreamWindowAllocationFailure<'route> {
    window: Box<ConfirmedStagingWindow<'route>>,
    error: Error,
}

/// Borrowed only from the consuming operation below. Both this custody permit
/// and the node-owned exact-store/section authority are required for mutation.
pub(in crate::node_runtime) struct StreamWindowAllocationPermit<'a, 'route> {
    window: &'a ConfirmedStagingWindow<'route>,
    request: &'a PrepareStreamUploadSegmentAppendReq,
    expected: &'a ExpectedStreamAppendState,
}

impl<'route> ConfirmedStagingWindow<'route> {
    pub(in crate::node_runtime) fn allocate_stream_embedded(
        self,
        section: &EmbeddedPublicationSection<'_>,
        request: &PrepareStreamUploadSegmentAppendReq,
        expected: &ExpectedStreamAppendState,
        #[cfg(test)] hook: impl FnMut(&'static str) -> Result<(), StoreError>,
    ) -> Result<AllocatedStreamWindow<'route>, StreamWindowAllocationFailure<'route>> {
        let permit = StreamWindowAllocationPermit {
            window: &self,
            request,
            expected,
        };
        let result = section.allocate_confirmed_stream_window(
            &permit,
            #[cfg(test)]
            hook,
        );
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

impl StreamWindowAllocationPermit<'_, '_> {
    pub(in crate::node_runtime) fn require_section(
        &self,
        section: &EmbeddedPublicationSection<'_>,
    ) -> Result<(), Error> {
        self.window.require_valid()?;
        self.window
            .terminal
            .owner
            .route
            .require_embedded_section(section)?;
        Ok(())
    }

    pub(in crate::node_runtime) fn deadline(&self) -> Instant {
        self.window.read.deadline()
    }

    pub(in crate::node_runtime) fn publication_request(
        &self,
    ) -> Result<CompletePublicationRequest<'_>, Error> {
        self.window
            .terminal
            .owner
            .sender
            .source
            .request(self.window.terminal.owner.route.primary())
            .map_err(|_| {
                Error::Window("stream allocation requires retained complete context".into())
            })
    }

    pub(in crate::node_runtime) fn require_effect(
        &self,
        pg: &PgStore,
        authority: &EmbeddedPublicationAuthority<'_>,
        request: &CompletePublicationRequest<'_>,
    ) -> Result<(), Error> {
        self.window.require_valid()?;
        authority.require_stream_window_allocation(self)?;
        authority.require_effect(pg, request)?;
        let original = self.publication_request()?;
        if original.id() != request.id() || original.command_sha256() != request.command_sha256() {
            return Err(Error::Window(
                "stream allocation complete command identity".into(),
            ));
        }
        Ok(())
    }

    pub(in crate::node_runtime::pg_store) fn require_prefix(
        &self,
        pg: &PgStore,
        request: &CompletePublicationRequest<'_>,
    ) -> Result<(), Error> {
        self.window
            .read
            .require_store_prefix(pg, request.node_id(), request.topology())
    }

    pub(in crate::node_runtime::pg_store) fn prepare_in_open_transaction(
        &self,
        pg: &PgStore,
        authority: &EmbeddedPublicationAuthority<'_>,
        request: &CompletePublicationRequest<'_>,
        placement: PayloadPlacementInput<'_>,
    ) -> Result<StreamUploadSegmentRecord, Error> {
        self.require_effect(pg, authority, request)?;
        match &self.window.terminal.owner.sender.source {
            PublicationSource::Inspected(evidence) => evidence
                .pending
                .as_ref()
                .expect("terminal context")
                .0
                .require_stream_allocation_window(pg, self.request)?,
            PublicationSource::Submitted { evidence, .. } => {
                evidence.require_stream_allocation_window(pg, self.request)?
            }
        }
        crate::node_runtime::pg_store::payload_staging::prepare_confirmed_stream_window(
            pg, placement, self, authority, request,
        )
    }

    pub(in crate::node_runtime::pg_store) fn preparation_inputs(
        &self,
    ) -> (
        &PrepareStreamUploadSegmentAppendReq,
        &ExpectedStreamAppendState,
        crate::types::AdmittedRouteEffectFence,
    ) {
        (self.request, self.expected, self.window.read.effect_fence())
    }
}

impl AllocatedStreamWindow<'_> {
    #[cfg(test)]
    pub(in crate::node_runtime::pg_store) fn test_binding_state(
        &self,
    ) -> (Instant, Option<ContextSubmissionObservation>) {
        (
            self.window.read.deadline(),
            self.window.installation_observation(),
        )
    }

    pub(in crate::node_runtime) fn id(&self) -> MetadataCommandId {
        self.window.id()
    }
    #[cfg(test)]
    pub(in crate::node_runtime::pg_store) fn segment(&self) -> &StreamUploadSegmentRecord {
        &self.segment
    }
}

impl StreamWindowAllocationFailure<'_> {
    #[cfg(test)]
    pub(in crate::node_runtime::pg_store) fn test_installation_observation(
        &self,
    ) -> Option<ContextSubmissionObservation> {
        self.window.installation_observation()
    }

    pub(in crate::node_runtime) fn id(&self) -> MetadataCommandId {
        self.window.id()
    }
    pub(in crate::node_runtime) fn error(&self) -> &Error {
        &self.error
    }
}
