// Copyright The Argmin Authors.
// SPDX-License-Identifier: Apache-2.0

//! Fresh primary capture into complete submission ownership. No descriptor or
//! installable inventory leaves the capture snapshot independently. This only
//! reads: there is no acceptance history to recover on failure. Never use this
//! to retry/reissue an accepted or uncertain command; retain its original holder.

use super::super::super::capture::{StagingCapture, StagingCaptureError};
use super::super::super::command_context::{ContextError, ContextualCommand};
use super::ContextualSubmission;
use crate::metadata_command::{BucketWriteReservationProof, MetadataCommandEnvelope};
use crate::node_runtime::pg_store::PgStore;
use crate::NodeId;

#[derive(Debug, thiserror::Error)]
pub(in super::super::super::super) enum FreshContextSubmissionError {
    #[error(transparent)]
    Capture(#[from] StagingCaptureError),
    #[error(transparent)]
    Context(#[from] ContextError),
}

enum FreshInput<'a> {
    Introduction(MetadataCommandEnvelope, &'a BucketWriteReservationProof),
    Append(MetadataCommandEnvelope),
}

impl StagingCapture<'_> {
    /// The node holds its usual PG serialization. Capture the exact fresh
    /// command/proof and required pending inventory in one read-only SQL view.
    /// The bucket proof still requires the existing durable fanout validation.
    /// This neither installs a command nor transfers frontend resource custody.
    pub(in super::super::super::super) fn introduction_submission(
        &self,
        pg: &PgStore,
        node_id: NodeId,
        command: MetadataCommandEnvelope,
        proof: &BucketWriteReservationProof,
    ) -> Result<ContextualSubmission, FreshContextSubmissionError> {
        self.fresh_submission(pg, node_id, FreshInput::Introduction(command, proof))
    }

    /// Select the prepared retained window and capture its lookahead without
    /// allocating, selecting payload, refreshing an old descriptor or writing.
    pub(in super::super::super::super) fn append_submission(
        &self,
        pg: &PgStore,
        node_id: NodeId,
        command: MetadataCommandEnvelope,
    ) -> Result<ContextualSubmission, FreshContextSubmissionError> {
        self.fresh_submission(pg, node_id, FreshInput::Append(command))
    }

    fn fresh_submission(
        &self,
        pg: &PgStore,
        node_id: NodeId,
        input: FreshInput<'_>,
    ) -> Result<ContextualSubmission, FreshContextSubmissionError> {
        pg.require_usable_connection()
            .map_err(StagingCaptureError::from)?;
        self.effect_fence
            .require_valid_for(self.effect_fence.cluster_epoch())
            .map_err(StagingCaptureError::from)?;
        pg.require_live_allocation_lifetime()
            .map_err(StagingCaptureError::from)?;
        let snapshot = pg
            .command_build_snapshot()
            .map_err(StagingCaptureError::from)?;
        // Candidate capture rechecks the fence after snapshot entry and pins
        // the view with its prefix read, including empty-payload introductions.
        let candidate = match input {
            FreshInput::Introduction(command, proof) => {
                self.introduction_candidate(pg, node_id, command, proof)?
            }
            FreshInput::Append(command) => self.append_candidate(pg, node_id, command)?,
        };
        let context = ContextualCommand::from_described(candidate);
        context.observe_pending_inventory_in_open_transaction(pg, self.topology)?;
        snapshot.finish().map_err(StagingCaptureError::from)?;
        // Finishing the read transaction may itself wait. Do not return fresh
        // work after its immutable fence/lifetime has expired. Installation
        // must still rederive inventory and check authority at its own effect.
        pg.require_live_allocation_lifetime()
            .map_err(StagingCaptureError::from)?;
        self.effect_fence
            .require_valid_for(self.effect_fence.cluster_epoch())
            .map_err(StagingCaptureError::from)?;
        Ok(context.into_submission())
    }
}
