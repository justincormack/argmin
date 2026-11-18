// Copyright The Argmin Authors.
// SPDX-License-Identifier: Apache-2.0

//! Consuming receipt-to-manifest handoff. The manifest cannot be extracted or
//! substituted, and cannot authorize publication from an old metadata snapshot.
use super::*;
use crate::node_runtime::pg_store::payload_staging::DirectStagingManifest;

#[path = "terminal.rs"]
mod terminal;
pub(in crate::node_runtime) use terminal::{
    DirectTerminalInstallation, DirectTerminalInstallationObservation,
    DirectTerminalInstallationProof, DirectTerminalPreparation,
};

#[must_use]
pub(in crate::node_runtime) struct WitnessedDirectPayload<'route> {
    prepared: PreparedAcknowledgedDirectPayload<'route>,
    manifest: DirectStagingManifest,
}

#[must_use]
pub(in crate::node_runtime) struct DirectPayloadIntegrityFailure<'route> {
    prepared: Box<PreparedAcknowledgedDirectPayload<'route>>,
    error: Error,
}

impl<'route> DirectPayloadIntegrityFailure<'route> {
    pub(in crate::node_runtime) fn error(&self) -> &Error {
        &self.error
    }
    pub(in crate::node_runtime) fn into_prepared(
        self,
    ) -> PreparedAcknowledgedDirectPayload<'route> {
        *self.prepared
    }
}

impl<'route> PreparedAcknowledgedDirectPayload<'route> {
    /// CRC is an input claim, not evidence. Match it against the complete
    /// recorded systematic receipts, including canonical EC padding. All other
    /// physical fields come from the original owner/window and file journal.
    pub(in crate::node_runtime) fn with_integrity(
        self,
        segment_crc64: u64,
    ) -> Result<WitnessedDirectPayload<'route>, DirectPayloadIntegrityFailure<'route>> {
        let result = (|| {
            let permit = DirectStagingPreparation {
                custody: &self.custody,
            };
            permit.require_valid()?;
            let writer = self.custody.writer();
            let manifest = writer
                .acknowledgements
                .direct_manifest(&writer.window, segment_crc64)?;
            permit.require_valid()?;
            Ok(manifest)
        })();
        match result {
            Ok(manifest) => Ok(WitnessedDirectPayload {
                prepared: self,
                manifest,
            }),
            Err(error) => Err(DirectPayloadIntegrityFailure {
                prepared: Box::new(self),
                error,
            }),
        }
    }
}

impl WitnessedDirectPayload<'_> {
    /// Borrow the original storage-minted subject for terminal DEK/checksum
    /// rewrapping; callers cannot replace it with an independently minted one.
    pub(in crate::node_runtime) fn encryption_subject(&self) -> &crate::ObjectEncryptionSubject {
        self.prepared.custody.writer().encryption_subject()
    }
    #[cfg(test)]
    pub(in crate::node_runtime::pg_store) fn test_cross_snapshot(&mut self) {
        self.prepared.captured.snapshot.auth_snapshot.existing_etag =
            Some("crossed snapshot".into());
    }
    pub(in crate::node_runtime) fn id(&self) -> MetadataCommandId {
        self.prepared.id()
    }
    pub(in crate::node_runtime) fn auth_snapshot(&self) -> &crate::DirectPutCommitSnapshot {
        self.prepared.auth_snapshot()
    }
    #[cfg(test)]
    pub(in crate::node_runtime::pg_store) fn test_manifest(&self) -> &DirectStagingManifest {
        &self.manifest
    }
    #[cfg(test)]
    pub(in crate::node_runtime::pg_store) fn test_binding_state(
        &self,
    ) -> (Instant, Option<ContextSubmissionObservation>) {
        self.prepared.test_binding_state()
    }
}
