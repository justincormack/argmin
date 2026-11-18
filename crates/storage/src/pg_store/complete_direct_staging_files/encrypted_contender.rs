// Copyright The Argmin Authors.
// SPDX-License-Identifier: Apache-2.0

//! Capture one exact contender without detaching this request's encrypted
//! payload, original deadline or accounting.
use super::*;
use crate::node_runtime::engine::EmbeddedPublicationAuthority;
use crate::node_runtime::pg_store::{
    CompletePendingEvidence, CompletePendingRequest, CompletePublicationRequest, PgStore,
};

#[path = "encrypted_contender_drain.rs"]
mod drain;

#[must_use]
pub(in crate::node_runtime) struct DirectContenderInspection<'route> {
    payload: AcknowledgedEncryptedDirectPayload<'route>,
    contender: CompletePendingEvidence,
}

impl<'route> AcknowledgedEncryptedDirectPayload<'route> {
    pub(in crate::node_runtime) fn inspect_contender_embedded(
        mut self,
        section: &EmbeddedPublicationSection<'_>,
    ) -> Result<DirectContenderInspection<'route>, StagingFileHandoffFailure<Self>> {
        if let Err(error) = self.work.begin_attempt() {
            return Err(StagingFileHandoffFailure::new(self, error.into()));
        }
        let permit = DirectStagingPreparation {
            custody: &self.custody,
        };
        match section.inspect_direct_contender(&permit) {
            Ok(contender) => Ok(DirectContenderInspection {
                payload: self,
                contender,
            }),
            Err(error) => Err(StagingFileHandoffFailure::new(self, error)),
        }
    }
}

impl<'route> DirectContenderInspection<'route> {
    #[cfg(test)]
    pub(in crate::node_runtime::pg_store) fn test_attempts(&self) -> usize {
        self.payload.work.test_attempts()
    }

    #[cfg(test)]
    pub(in crate::node_runtime::pg_store) fn test_contender_bytes(&self) -> Option<Vec<u8>> {
        self.contender
            .context()
            .map(|context| context.encode_command().unwrap().bytes().to_vec())
    }

    pub(in crate::node_runtime) fn contender_id(&self) -> Option<MetadataCommandId> {
        self.contender.id()
    }

    pub(in crate::node_runtime) fn publication_started(&self) -> bool {
        self.contender.publication_started()
    }

    /// Clear is an observation, not installation authority. Snapshot inspection,
    /// request validation and installation still recheck the admitted section.
    /// A present contender cannot be discarded through this transition.
    pub(in crate::node_runtime) fn into_clear_payload(
        self,
    ) -> Result<AcknowledgedEncryptedDirectPayload<'route>, StagingFileHandoffFailure<Self>> {
        if self.contender.id().is_some() {
            return Err(StagingFileHandoffFailure::new(
                self,
                Error::Window("direct PUT retains an undrained contender".into()),
            ));
        }
        Ok(self.payload)
    }
}

impl DirectStagingPreparation<'_, '_> {
    pub(in crate::node_runtime) fn capture_contender_in_store(
        &self,
        pg: &PgStore,
        authority: &EmbeddedPublicationAuthority<'_>,
        request: &CompletePublicationRequest<'_>,
    ) -> Result<CompletePendingEvidence, Error> {
        self.require_valid()?;
        authority.require_direct_preparation(self)?;
        authority.require_effect(pg, request)?;
        let original = self.publication_request()?;
        if original.id() != request.id() || original.command_sha256() != request.command_sha256() {
            return Err(Error::Window("direct contender original command".into()));
        }
        let pending = CompletePendingRequest::new(
            request.id().pg_id(),
            request.id().cluster_epoch(),
            request.topology(),
        )
        .map_err(|e| Error::Window(e.to_string()))?
        .read_current_slot(pg, request.node_id())
        .map_err(|e| Error::Window(e.to_string()))?;
        if pending.id() == Some(original.id()) {
            return Err(Error::Window(
                "direct introduction slot was not retired".into(),
            ));
        }
        self.require_valid()?;
        authority.require_direct_preparation(self)?;
        authority.require_effect(pg, request)?;
        pg.require_live_allocation_lifetime()?;
        Ok(pending)
    }
}
