// Copyright The Argmin Authors.
// SPDX-License-Identifier: Apache-2.0

//! Close physical writing without discarding any original command/window custody.
//! This is evidence for subsequent publication, not new mutation authority.
use super::*;

#[path = "prepare.rs"]
mod preparation;
pub(in crate::node_runtime) use preparation::{
    StreamAppendInstallation, StreamAppendInstallationObservation, StreamAppendInstallationProof,
    StreamAppendPreparation,
};

impl AcknowledgedStreamStagingSegment<'_> {
    fn writer(&self) -> &StreamStagingFileWriter<'_> {
        match &self.custody {
            Custody::Embedded(writer) => writer,
            Custody::Remote(writer) => &writer.writer,
        }
    }
}

#[must_use = "retains cleanup custody until the subsequent publication handoff"]
pub(in crate::node_runtime) struct AcknowledgedStreamStagingSegment<'route> {
    custody: Custody<'route>,
}

enum Custody<'route> {
    Embedded(StreamStagingFileWriter<'route>),
    Remote(RemoteStreamStagingFileWriter<'route>),
}

impl AcknowledgedStreamStagingSegment<'_> {
    /// Original introduction/window command identity, not a new terminal command.
    pub(in crate::node_runtime) fn id(&self) -> MetadataCommandId {
        match &self.custody {
            Custody::Embedded(writer) => writer.id(),
            Custody::Remote(writer) => writer.writer.id(),
        }
    }

    #[cfg(test)]
    pub(in crate::node_runtime::pg_store) fn test_binding_state(
        &self,
    ) -> (Instant, Option<ContextSubmissionObservation>) {
        match &self.custody {
            Custody::Embedded(writer) => writer.test_binding_state(),
            Custody::Remote(writer) => writer.test_binding_state(),
        }
    }
}

impl<'route> StreamStagingFileWriter<'route> {
    /// Consume a fully acknowledged single allocated stream segment. No network, SQL, cleanup,
    /// re-admission or cutoff renewal occurs here. Evidence survives expiry;
    /// later command construction/installation must check its own authority.
    pub(in crate::node_runtime) fn into_acknowledged(
        self,
    ) -> Result<AcknowledgedStreamStagingSegment<'route>, StagingFileHandoffFailure<Self>> {
        if let Err(error) = self.acknowledgements.require_confirmed() {
            return Err(StagingFileHandoffFailure::new(self, error));
        }
        Ok(AcknowledgedStreamStagingSegment {
            custody: Custody::Embedded(self),
        })
    }
}

impl<'route> RemoteStreamStagingFileWriter<'route> {
    pub(in crate::node_runtime) fn into_acknowledged(
        self,
    ) -> Result<AcknowledgedStreamStagingSegment<'route>, StagingFileHandoffFailure<Self>> {
        if let Err(error) = self.writer.acknowledgements.require_confirmed() {
            return Err(StagingFileHandoffFailure::new(self, error));
        }
        Ok(AcknowledgedStreamStagingSegment {
            custody: Custody::Remote(self),
        })
    }
}
