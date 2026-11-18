// Copyright The Argmin Authors.
// SPDX-License-Identifier: Apache-2.0

//! Read-only post-write snapshot. This cannot detach acknowledged physical
//! custody, mint a terminal command or authorize installation from stale facts.
use super::*;
use crate::node_runtime::engine::EmbeddedPublicationAuthority;
use crate::node_runtime::pg_store::{CompletePublicationRequest, PgStore};

#[path = "integrity.rs"]
mod integrity;
#[path = "prepare_remote.rs"]
mod remote;
pub(in crate::node_runtime) use integrity::{
    DirectTerminalInstallation, DirectTerminalInstallationObservation,
    DirectTerminalInstallationProof, DirectTerminalPreparation,
};

pub(in crate::node_runtime) struct DirectStagingSnapshot {
    snapshot: crate::DirectPutCommitStorageSnapshot,
}

#[must_use]
pub(in crate::node_runtime) struct PreparedAcknowledgedDirectPayload<'route> {
    custody: AcknowledgedDirectStagingPayload<'route>,
    captured: DirectStagingSnapshot,
}

#[must_use]
pub(in crate::node_runtime) struct DirectStagingPreparationFailure<'route> {
    custody: Box<AcknowledgedDirectStagingPayload<'route>>,
    error: Error,
}

impl<'route> DirectStagingPreparationFailure<'route> {
    pub(in crate::node_runtime) fn error(&self) -> &Error {
        &self.error
    }
    pub(in crate::node_runtime) fn into_acknowledged(
        self,
    ) -> AcknowledgedDirectStagingPayload<'route> {
        *self.custody
    }
}

/// Only a live consuming caller can lend this permit. No independent subject,
/// descriptor, store, acknowledgement batch or caller deadline can mint it.
pub(in crate::node_runtime) struct DirectStagingPreparation<'a, 'route> {
    custody: &'a AcknowledgedDirectStagingPayload<'route>,
}

impl<'route> AcknowledgedDirectStagingPayload<'route> {
    pub(in crate::node_runtime) fn prepare_direct_embedded(
        self,
        section: &EmbeddedPublicationSection<'_>,
    ) -> Result<PreparedAcknowledgedDirectPayload<'route>, DirectStagingPreparationFailure<'route>>
    {
        let captured = section
            .prepare_acknowledged_direct_payload(&DirectStagingPreparation { custody: &self });
        match captured {
            Ok(captured) => Ok(PreparedAcknowledgedDirectPayload {
                custody: self,
                captured,
            }),
            Err(error) => Err(DirectStagingPreparationFailure {
                custody: Box::new(self),
                error,
            }),
        }
    }
}

impl PreparedAcknowledgedDirectPayload<'_> {
    /// The introduction identity, not a newly allocated publication ID.
    pub(in crate::node_runtime) fn id(&self) -> MetadataCommandId {
        self.custody.id()
    }
    /// Logical request facts only; not a detached placement/snapshot authority.
    pub(in crate::node_runtime) fn auth_snapshot(&self) -> &crate::DirectPutCommitSnapshot {
        &self.captured.snapshot.auth_snapshot
    }
    #[cfg(test)]
    pub(in crate::node_runtime::pg_store) fn test_binding_state(
        &self,
    ) -> (Instant, Option<ContextSubmissionObservation>) {
        self.custody.test_binding_state()
    }
    #[cfg(test)]
    pub(in crate::node_runtime::pg_store) fn test_snapshot(
        &self,
    ) -> &crate::DirectPutCommitStorageSnapshot {
        &self.captured.snapshot
    }
}

impl DirectStagingPreparation<'_, '_> {
    pub(in crate::node_runtime) fn require_section(
        &self,
        section: &EmbeddedPublicationSection<'_>,
    ) -> Result<(), Error> {
        self.require_valid()?;
        self.custody
            .writer()
            .window
            .terminal
            .owner
            .route
            .require_embedded_section(section)?;
        Ok(())
    }
    fn require_valid(&self) -> Result<(), Error> {
        let writer = self.custody.writer();
        if let Custody::Remote(remote) = &self.custody.custody {
            if crate::clock::current_time_millis() >= remote.cutoff.portable_wall_valid_until_ms {
                return Err(StoreError::OperationDeadlineExceeded {
                    context: "remote direct snapshot original cutoff",
                }
                .into());
            }
        }
        writer.window.require_valid()?;
        writer
            .fence
            .require_valid_for(self.custody.id().cluster_epoch())?;
        if Instant::now() >= writer.deadline {
            return Err(StoreError::OperationDeadlineExceeded {
                context: "acknowledged direct payload preparation",
            }
            .into());
        }
        writer.acknowledgements.require_confirmed()
    }
    pub(in crate::node_runtime) fn deadline(&self) -> Instant {
        self.custody.writer().deadline
    }
    pub(in crate::node_runtime) fn publication_request(
        &self,
    ) -> Result<CompletePublicationRequest<'_>, Error> {
        let owner = &self.custody.writer().window.terminal.owner;
        owner
            .sender
            .source
            .request(owner.route.primary())
            .map_err(|e| Error::Window(e.to_string()))
    }
    pub(in crate::node_runtime) fn capture_in_store(
        &self,
        pg: &PgStore,
        authority: &EmbeddedPublicationAuthority<'_>,
        request: &CompletePublicationRequest<'_>,
    ) -> Result<DirectStagingSnapshot, Error> {
        self.require_valid()?;
        authority.require_direct_preparation(self)?;
        authority.require_effect(pg, request)?;
        let original = self.publication_request()?;
        if original.id() != request.id() || original.command_sha256() != request.command_sha256() {
            return Err(Error::Window("direct preparation original command".into()));
        }
        let (deadline, route_fence) = authority.direct_reinspection_bounds(self)?;
        pg.validate_complete_direct_reinspection_prefix(
            request.node_id(),
            request.id().cluster_epoch(),
            request.topology(),
            deadline.min(self.deadline()),
            route_fence.intersect(self.custody.writer().fence)?,
        )?;
        self.require_valid()?;
        authority.require_effect(pg, request)?;
        let snapshot = pg
            .command_build_snapshot()
            .map_err(|e| Error::Window(e.to_string()))?;
        let captured = self.custody.writer().window.read.direct_commit_snapshot(
            pg,
            request.node_id(),
            request.topology(),
        )?;
        snapshot
            .finish()
            .map_err(|e| Error::Window(e.to_string()))?;
        self.require_valid()?;
        authority.require_direct_preparation(self)?;
        authority.require_effect(pg, request)?;
        pg.require_live_allocation_lifetime()?;
        Ok(DirectStagingSnapshot { snapshot: captured })
    }
}
