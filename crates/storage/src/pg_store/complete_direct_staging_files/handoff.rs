// Copyright The Argmin Authors.
// SPDX-License-Identifier: Apache-2.0

//! Close physical writing without discarding any original command/window custody.
//! This is evidence for subsequent publication, not new mutation authority.
use super::*;

#[path = "prepare.rs"]
mod preparation;
pub(in crate::node_runtime) use preparation::{
    DirectStagingPreparation, DirectStagingSnapshot, DirectTerminalInstallation,
    DirectTerminalInstallationObservation, DirectTerminalInstallationProof,
    DirectTerminalPreparation,
};

#[must_use = "retains cleanup custody until the subsequent publication handoff"]
pub(in crate::node_runtime) struct AcknowledgedDirectStagingPayload<'route> {
    custody: Custody<'route>,
    // One newly acquired post-file section; never the released file-binding section.
    preparation_session: Option<crate::node_client::MetadataCommandSessionIdentity>,
}

enum Custody<'route> {
    Embedded(DirectStagingFileWriter<'route>),
    Remote(RemoteDirectStagingFileWriter<'route>),
}

impl<'route> AcknowledgedDirectStagingPayload<'route> {
    fn writer(&self) -> &DirectStagingFileWriter<'route> {
        match &self.custody {
            Custody::Embedded(writer) => writer,
            Custody::Remote(writer) => &writer.writer,
        }
    }
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

impl<'route> DirectStagingFileWriter<'route> {
    /// Consume a fully acknowledged Direct PUT payload. No network, SQL, cleanup,
    /// re-admission or cutoff renewal occurs here. Evidence survives expiry;
    /// later command construction/installation must check its own authority.
    pub(in crate::node_runtime) fn into_acknowledged(
        self,
    ) -> Result<AcknowledgedDirectStagingPayload<'route>, StagingFileHandoffFailure<Self>> {
        if let Err(error) = self.acknowledgements.require_confirmed() {
            return Err(StagingFileHandoffFailure::new(self, error));
        }
        Ok(AcknowledgedDirectStagingPayload {
            custody: Custody::Embedded(self),
            preparation_session: None,
        })
    }
}

impl<'route> RemoteDirectStagingFileWriter<'route> {
    pub(in crate::node_runtime) fn into_acknowledged(
        self,
    ) -> Result<AcknowledgedDirectStagingPayload<'route>, StagingFileHandoffFailure<Self>> {
        if let Err(error) = self.writer.acknowledgements.require_confirmed() {
            return Err(StagingFileHandoffFailure::new(self, error));
        }
        Ok(AcknowledgedDirectStagingPayload {
            custody: Custody::Remote(self),
            preparation_session: None,
        })
    }
}
