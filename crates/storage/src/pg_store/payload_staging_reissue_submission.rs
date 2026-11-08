// Copyright The Argmin Authors.
// SPDX-License-Identifier: Apache-2.0

//! Keep both possible commands through an uncertain same-epoch replacement.
//! This only records local observations. The enclosing recovery boundary must
//! supply admission/serialization and authorize reissue; no such authority is
//! minted here, and v13 still does not authenticate the retained descriptor.

use super::{InstallationObservation, StagingSubmission, SubmissionError};
use crate::metadata_command::MetadataCommandId;
use crate::node_runtime::pg_store::payload_staging::candidate::{
    CandidateBindingError, DescribedStagingCommand, StagingCommandInspection,
};
use crate::node_runtime::pg_store::{PendingMetadataCommandSlotReplaceError, PgStore};
use crate::PgTopology;

pub(in super::super::super) struct ReissuePreparationFailure {
    submission: Box<StagingSubmission>,
    error: CandidateBindingError,
}

impl std::fmt::Debug for ReissuePreparationFailure {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("ReissuePreparationFailure")
            .field("error", &self.error)
            .finish_non_exhaustive()
    }
}

impl ReissuePreparationFailure {
    pub(in super::super::super) fn error(&self) -> &CandidateBindingError {
        &self.error
    }

    pub(in super::super::super) fn into_submission(self) -> StagingSubmission {
        *self.submission
    }
}

/// No clone, raw-pair constructor or consuming split. An uncertain replacement
/// cannot lose its source by returning only the proposed successor.
pub(in super::super::super) struct StagingReissue {
    // Only this owning submission module and its private children may move
    // the pair into the complete-context holder. Never expose these to callers.
    pub(super) source: StagingSubmission,
    pub(super) successor: DescribedStagingCommand,
    pub(super) observation: InstallationObservation,
}

impl StagingSubmission {
    /// Derive a same-payload candidate with the ORIGINAL descriptor/topology.
    /// Rejection returns the complete source and its prior history unchanged.
    /// The SQL engine still checks the exact next durable log position.
    pub(in super::super::super) fn into_same_epoch_reissue(
        self,
        id: MetadataCommandId,
    ) -> Result<StagingReissue, ReissuePreparationFailure> {
        let successor = if id.cluster_epoch() == self.description.command().id().cluster_epoch() {
            self.description.reissued_at(id)
        } else {
            Err(CandidateBindingError::ReissuePosition)
        };
        match successor {
            Ok(successor) => Ok(StagingReissue {
                source: self,
                successor,
                observation: InstallationObservation::NoAcceptanceObserved,
            }),
            Err(error) => Err(ReissuePreparationFailure {
                submission: Box::new(self),
                error,
            }),
        }
    }
}

impl StagingReissue {
    pub(in super::super::super) fn source(&self) -> &StagingSubmission {
        &self.source
    }

    pub(in super::super::super) fn successor(&self) -> StagingCommandInspection<'_> {
        self.successor.inspect()
    }

    pub(in super::super::super) fn observation(&self) -> InstallationObservation {
        self.observation
    }

    /// One replacement attempt, never an unbounded retry loop. False and a
    /// definitive error describe THIS call, not absence of earlier acceptance.
    /// Neither can erase an unconfirmed replacement or drop either description.
    pub(in super::super::super) fn replace(
        &mut self,
        pg: &PgStore,
        node_id: u32,
        topology: &PgTopology,
    ) -> Result<bool, SubmissionError> {
        if let Err(error) = pg.require_usable_connection() {
            self.observation = InstallationObservation::Unconfirmed;
            return Err(SubmissionError::Unconfirmed(error));
        }
        if !pg.conn.is_autocommit() {
            return Err(SubmissionError::OpenTransaction);
        }
        let bound = self.source.description.bind(topology)?;
        let previous = self.observation;
        self.observation = InstallationObservation::Unconfirmed;
        match pg.replace_bound_staging_pending_metadata_command_slot_for_reissue(
            node_id,
            &bound.pending_protection(),
            self.successor.command(),
            topology,
            Some(self.successor.command().bucket_name()),
        ) {
            Ok(true) => {
                self.observation = InstallationObservation::Installed;
                Ok(true)
            }
            Ok(false) => {
                self.observation = previous;
                Ok(false)
            }
            Err(PendingMetadataCommandSlotReplaceError::Definitive(error)) => {
                self.observation = previous;
                Err(SubmissionError::Rejected(error))
            }
            Err(PendingMetadataCommandSlotReplaceError::MayHaveApplied(error)) => {
                Err(SubmissionError::Unconfirmed(error))
            }
        }
    }

    /// Select the successor only after an observed committed replacement/exact
    /// retry. Otherwise retain the complete pair, including source history.
    /// This is NOT current slot presence, replicated custody or cleanup/recovery
    /// permission. Those still require the authoritative convergence boundary.
    pub(in super::super::super) fn into_observed_successor(
        self,
    ) -> Result<StagingSubmission, Box<Self>> {
        if self.observation != InstallationObservation::Installed {
            return Err(Box::new(self));
        }
        Ok(StagingSubmission {
            description: self.successor,
            observation: InstallationObservation::Installed,
        })
    }
}
