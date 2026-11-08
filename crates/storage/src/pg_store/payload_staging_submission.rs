// Copyright The Argmin Authors.
// SPDX-License-Identifier: Apache-2.0

//! Process-local installation history attached to the complete description.
//! This is preparatory plumbing, NOT a durable custody certificate. In
//! particular, v13 command bytes still do not authenticate the descriptor.
//! Production must switch its mandatory carrier and frontend custody together.

use super::candidate::{CandidateBindingError, DescribedStagingCommand, StagingCommandInspection};
use crate::node_runtime::pg_store::{PendingMetadataCommandSlotInsertError, PgStore};
use crate::types::AdmittedRouteEffectFence;
use crate::{PgTopology, StoreError};
use std::time::Instant;

#[path = "payload_staging_reissue_submission.rs"]
pub(in super::super) mod reissue;

#[path = "payload_staging_context_submission.rs"]
mod contextual;

#[cfg(test)]
pub(in super::super) use contextual::{
    ContextSubmissionObservation, ContextualEpochReissue, ContextualPartAbandonment,
    ContextualPartAbandonmentReissue, ContextualReissue, ContextualReissueInspectionFailure,
    ContextualReissuePreparationFailure, ContextualSubmission,
    ContextualSubmissionPreparationFailure, EpochReissueInspectionFailure,
    FreshContextSubmissionError, PartAbandonmentPreparationFailure, PartAbandonmentReissueFailure,
    PartAbandonmentReissueInspectionFailure, PreparedContextualReissue,
    PreparedContextualSubmission, PreparedEpochReissue, PreparedPartAbandonmentReissue,
};

/// History for this submission, not a current reference/absence verdict or
/// permission to release resources. Even NoAcceptanceObserved does not prove
/// that another caller has never installed this command.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(in super::super) enum InstallationObservation {
    NoAcceptanceObserved,
    Unconfirmed,
    /// An installation committed or an exact slot plus its indexes was verified
    /// in a committed transaction. Does not assert owner application, current
    /// slot presence, replicated acceptance or authenticated descriptor custody.
    Installed,
}

#[derive(Debug, thiserror::Error)]
pub(in super::super) enum SubmissionError {
    #[error("staging submission cannot confirm installation inside an outer transaction")]
    OpenTransaction,
    #[error(transparent)]
    Binding(#[from] CandidateBindingError),
    /// Per-call rejection only; consult retained history for earlier attempts.
    #[error("staging pending installation rejected this attempt: {0}")]
    Rejected(StoreError),
    #[error("staging pending installation is unconfirmed: {0}")]
    Unconfirmed(StoreError),
}

/// Own the command AND descriptor throughout attempts, errors and unwinding.
/// No Clone, mutable description, consuming split, reset or cleanup-on-Drop.
/// Inspection never lends the complete candidate, so it cannot derive a new
/// submission that forgets this holder's prior installation observations.
pub(in super::super) struct StagingSubmission {
    description: DescribedStagingCommand,
    observation: InstallationObservation,
}

impl DescribedStagingCommand {
    pub(in super::super) fn into_submission(self) -> StagingSubmission {
        StagingSubmission {
            description: self,
            observation: InstallationObservation::NoAcceptanceObserved,
        }
    }
}

impl StagingSubmission {
    pub(in super::super) fn description(&self) -> StagingCommandInspection<'_> {
        self.description.inspect()
    }

    pub(in super::super) fn observation(&self) -> InstallationObservation {
        self.observation
    }

    /// The owning node supplies its configured topology and holds its usual PG
    /// serialization. Derive command and bucket scope from the bound description.
    /// Use the existing single transaction, including its final effect fences.
    /// Reject nested transactions: releasing a savepoint is not durable success.
    pub(in super::super) fn install(
        &mut self,
        pg: &PgStore,
        node_id: u32,
        configured_topology: &PgTopology,
        effect_fence: AdmittedRouteEffectFence,
        operation_deadline: Option<Instant>,
    ) -> Result<(), SubmissionError> {
        if let Err(error) = pg.require_usable_connection() {
            self.observation = InstallationObservation::Unconfirmed;
            return Err(SubmissionError::Unconfirmed(error));
        }
        if !pg.conn.is_autocommit() {
            return Err(SubmissionError::OpenTransaction);
        }
        let bound = self.description.bind(configured_topology)?;
        let previous = self.observation;
        // Set this BEFORE entering SQLite. Unwinding must not restore presumed
        // frontend ownership. This holder does no cleanup or transaction repair.
        self.observation = InstallationObservation::Unconfirmed;
        match pg.try_insert_bound_staging_pending_metadata_command_slot(
            node_id,
            &bound.pending_protection(),
            configured_topology,
            Some(self.description.command().bucket_name()),
            effect_fence,
            operation_deadline,
        ) {
            Ok(()) => {
                self.observation = InstallationObservation::Installed;
                Ok(())
            }
            Err(PendingMetadataCommandSlotInsertError::Definitive(error)) => {
                // A deadline, contender or rolled-back failure for THIS call cannot
                // erase an earlier uncertain or successfully observed install.
                self.observation = previous;
                Err(SubmissionError::Rejected(error))
            }
            Err(PendingMetadataCommandSlotInsertError::MayHaveApplied(error)) => {
                Err(SubmissionError::Unconfirmed(error))
            }
        }
    }
}
