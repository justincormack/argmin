// Copyright The Argmin Authors.
// SPDX-License-Identifier: Apache-2.0

//! Read-only preparation must retain the submission's installation history.
//! It is neither an installation attempt nor a custody/absence certificate.
//! No network work or cached-index installation belongs inside this scope.

use super::{ContextSubmissionObservation, ContextualSubmission, InstallationObservation};
use crate::metadata_command::MetadataCommandReplicaState;
use crate::node_runtime::pg_store::payload_staging::command_context::{
    ContextError, ContextPreparationFailure, PendingContextPreparation, PendingInventoryObservation,
};
use crate::node_runtime::pg_store::PgStore;
use crate::PgTopology;

/// Retains the original complete input plus history on preparation/finish
/// failure. No raw context, encoding, split or fresh-submission constructor.
pub(in super::super::super::super) struct ContextualSubmissionPreparationFailure {
    failure: ContextPreparationFailure,
    installation: InstallationObservation,
}

impl std::fmt::Debug for ContextualSubmissionPreparationFailure {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("ContextualSubmissionPreparationFailure")
            .field("error", self.failure.error())
            .field("installation", &self.installation)
            .finish_non_exhaustive()
    }
}

impl ContextualSubmissionPreparationFailure {
    pub(in super::super::super::super) fn error(&self) -> &ContextError {
        self.failure.error()
    }

    pub(in super::super::super::super) fn into_submission(self) -> ContextualSubmission {
        ContextualSubmission {
            context: self.failure.into_context(),
            installation: self.installation,
        }
    }
}

/// The inner preparation owns and drops the SQL snapshot before its context.
/// Only scalar diagnostics escape. Ending inspection returns the owning
/// submission with its unchanged history, never a bare descriptive context.
pub(in super::super::super::super) struct PreparedContextualSubmission<'pg> {
    preparation: PendingContextPreparation<'pg>,
    installation: InstallationObservation,
}

impl ContextualSubmission {
    /// Readiness inspection only: retain the exact source/context and history
    /// on every outcome. This never marks the follow-up installed or permits
    /// source retirement, and apply must independently revalidate its evidence.
    pub(in super::super::super::super) fn prepare_abandoned_part_pending<'pg>(
        self,
        pg: &'pg PgStore,
        node_id: u32,
        topology: &PgTopology,
    ) -> Result<PreparedContextualSubmission<'pg>, ContextualSubmissionPreparationFailure> {
        match self
            .context
            .prepare_abandoned_part_pending(pg, node_id, topology)
        {
            Ok(preparation) => Ok(PreparedContextualSubmission {
                preparation,
                installation: self.installation,
            }),
            Err(failure) => Err(ContextualSubmissionPreparationFailure {
                failure,
                installation: self.installation,
            }),
        }
    }

    pub(in super::super::super::super) fn prepare_pending<'pg>(
        self,
        pg: &'pg PgStore,
        topology: &PgTopology,
    ) -> Result<PreparedContextualSubmission<'pg>, ContextualSubmissionPreparationFailure> {
        match self.context.prepare_pending(pg, topology) {
            Ok(preparation) => Ok(PreparedContextualSubmission {
                preparation,
                installation: self.installation,
            }),
            Err(failure) => Err(ContextualSubmissionPreparationFailure {
                failure,
                installation: self.installation,
            }),
        }
    }
}

impl PreparedContextualSubmission<'_> {
    pub(in super::super::super::super) fn observation(&self) -> ContextSubmissionObservation {
        ContextSubmissionObservation {
            command_id: self.preparation.id(),
            installation: self.installation,
        }
    }

    /// Observed prefix only, not command acceptance, current slot custody or
    /// authoritative absence. In particular this never changes the history.
    pub(in super::super::super::super) fn prefix(&self) -> &MetadataCommandReplicaState {
        self.preparation.prefix()
    }

    pub(in super::super::super::super) fn inventory(&self) -> PendingInventoryObservation {
        self.preparation.inventory()
    }

    pub(in super::super::super::super) fn finish(
        self,
    ) -> Result<ContextualSubmission, ContextualSubmissionPreparationFailure> {
        match self.preparation.finish() {
            Ok(context) => Ok(ContextualSubmission {
                context,
                installation: self.installation,
            }),
            Err(failure) => Err(ContextualSubmissionPreparationFailure {
                failure,
                installation: self.installation,
            }),
        }
    }
}
