// Copyright The Argmin Authors.
// SPDX-License-Identifier: Apache-2.0

//! Reinspect an owning abandonment reissue in one read-only view. Reuse the
//! cleanup pair's snapshot rather than open a second snapshot for its finalizer.
//! Observations are not installation, source retirement or cleanup authority.

use super::super::super::{
    epoch_reissue::{EpochReissueInspectionFailure, PreparedEpochReissue},
    reissue_preparation::{ContextualReissueInspectionFailure, PreparedContextualReissue},
    ContextSubmissionObservation,
};
use super::{CleanupReissue, ContextError, ContextualPartAbandonmentReissue, Origin};
use crate::metadata_command::MetadataCommandReplicaState;
use crate::node_runtime::pg_store::payload_staging::command_context::PendingInventoryObservation;
use crate::node_runtime::pg_store::PgStore;
use crate::PgTopology;

enum PreparedCleanup<'pg> {
    SameEpoch(PreparedContextualReissue<'pg>),
    NewEpoch(PreparedEpochReissue<'pg>),
}

enum CleanupInspectionFailure {
    SameEpoch(ContextualReissueInspectionFailure),
    NewEpoch(EpochReissueInspectionFailure),
}

impl CleanupInspectionFailure {
    fn into_parts(self) -> (CleanupReissue, ContextError) {
        // The existing owning failures expose diagnostics by reference. Keep
        // them intact until the complete three-command failure is formed.
        match self {
            Self::SameEpoch(failure) => {
                let (reissue, error) = failure.into_parts();
                (CleanupReissue::SameEpoch(reissue), error)
            }
            Self::NewEpoch(failure) => {
                let (reissue, error) = failure.into_parts();
                (CleanupReissue::NewEpoch(reissue), error)
            }
        }
    }
}

/// Always retains the original finalizer and BOTH cleanup candidates. No
/// independently reusable context, pages or recovery certificate is exposed.
pub(in super::super::super::super::super::super) struct PartAbandonmentReissueInspectionFailure {
    reissue: Box<ContextualPartAbandonmentReissue>,
    error: ContextError,
}

impl std::fmt::Debug for PartAbandonmentReissueInspectionFailure {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("PartAbandonmentReissueInspectionFailure")
            .field("error", &self.error)
            .finish_non_exhaustive()
    }
}

impl PartAbandonmentReissueInspectionFailure {
    fn new(origin: Origin, cleanup: CleanupReissue, error: ContextError) -> Self {
        Self {
            reissue: Box::new(ContextualPartAbandonmentReissue { origin, cleanup }),
            error,
        }
    }

    fn from_cleanup(origin: Origin, failure: CleanupInspectionFailure) -> Self {
        let (cleanup, error) = failure.into_parts();
        Self::new(origin, cleanup, error)
    }

    pub(in super::super::super::super::super::super) fn error(&self) -> &ContextError {
        &self.error
    }

    pub(in super::super::super::super::super::super) fn into_reissue(
        self,
    ) -> ContextualPartAbandonmentReissue {
        *self.reissue
    }
}

/// Cleanup holds the sole snapshot and drops BEFORE the original finalizer.
/// All inspection output is scalar; finish returns the entire owning proposal.
pub(in super::super::super::super::super::super) struct PreparedPartAbandonmentReissue<'pg> {
    cleanup: PreparedCleanup<'pg>,
    origin: Origin,
    source_inventory: PendingInventoryObservation,
}

impl ContextualPartAbandonmentReissue {
    /// Caller holds normal PG serialization; no network calls in this scope.
    /// Authenticate the original abandoned source for both cleanup candidates,
    /// then inspect the retained finalizer in that SAME pinned read snapshot.
    /// Do not rebuild either cleanup from current mutable owner/session state.
    pub(in super::super::super::super::super::super) fn prepare_pending<'pg>(
        self,
        pg: &'pg PgStore,
        node_id: u32,
        topology: &PgTopology,
    ) -> Result<PreparedPartAbandonmentReissue<'pg>, PartAbandonmentReissueInspectionFailure> {
        let Self { origin, cleanup } = self;
        let prepared = match cleanup {
            CleanupReissue::SameEpoch(pair) => pair
                .prepare_abandoned_part_pending(pg, node_id, topology)
                .map(PreparedCleanup::SameEpoch)
                .map_err(CleanupInspectionFailure::SameEpoch),
            CleanupReissue::NewEpoch(pair) => pair
                .prepare_abandoned_part_pending(pg, node_id, topology)
                .map(PreparedCleanup::NewEpoch)
                .map_err(CleanupInspectionFailure::NewEpoch),
        };
        let cleanup = match prepared {
            Ok(cleanup) => cleanup,
            Err(error) => {
                return Err(PartAbandonmentReissueInspectionFailure::from_cleanup(
                    origin, error,
                ));
            }
        };
        match origin
            .source
            .context
            .observe_pending_inventory_in_open_transaction(pg, topology)
        {
            Ok(source_inventory) => Ok(PreparedPartAbandonmentReissue {
                cleanup,
                origin,
                source_inventory,
            }),
            Err(error) => match cleanup.finish() {
                Ok(cleanup) => Err(PartAbandonmentReissueInspectionFailure::new(
                    origin, cleanup, error,
                )),
                // Failed snapshot release takes precedence; never disguise it
                // as a completed inspection or drop the owning command set.
                Err(failure) => Err(PartAbandonmentReissueInspectionFailure::from_cleanup(
                    origin, failure,
                )),
            },
        }
    }
}

impl PreparedCleanup<'_> {
    fn finish(self) -> Result<CleanupReissue, CleanupInspectionFailure> {
        match self {
            Self::SameEpoch(prepared) => prepared
                .finish()
                .map(CleanupReissue::SameEpoch)
                .map_err(CleanupInspectionFailure::SameEpoch),
            Self::NewEpoch(prepared) => prepared
                .finish()
                .map(CleanupReissue::NewEpoch)
                .map_err(CleanupInspectionFailure::NewEpoch),
        }
    }
}

impl PreparedPartAbandonmentReissue<'_> {
    pub(in super::super::super::super::super::super) fn source(
        &self,
    ) -> ContextSubmissionObservation {
        self.origin.source.observation()
    }

    pub(in super::super::super::super::super::super) fn previous_cleanup(
        &self,
    ) -> ContextSubmissionObservation {
        match &self.cleanup {
            PreparedCleanup::SameEpoch(prepared) => prepared.source(),
            PreparedCleanup::NewEpoch(prepared) => prepared.source(),
        }
    }

    pub(in super::super::super::super::super::super) fn proposed_cleanup(
        &self,
    ) -> ContextSubmissionObservation {
        match &self.cleanup {
            PreparedCleanup::SameEpoch(prepared) => prepared.successor(),
            PreparedCleanup::NewEpoch(prepared) => prepared.successor(),
        }
    }

    /// The current inspection prefix, NOT the original proposal's capture.
    pub(in super::super::super::super::super::super) fn prefix(
        &self,
    ) -> &MetadataCommandReplicaState {
        match &self.cleanup {
            PreparedCleanup::SameEpoch(prepared) => prepared.prefix(),
            PreparedCleanup::NewEpoch(prepared) => prepared.prefix(),
        }
    }

    pub(in super::super::super::super::super::super) fn source_inventory(
        &self,
    ) -> PendingInventoryObservation {
        self.source_inventory
    }

    pub(in super::super::super::super::super::super) fn previous_cleanup_inventory(
        &self,
    ) -> PendingInventoryObservation {
        match &self.cleanup {
            PreparedCleanup::SameEpoch(prepared) => prepared.source_inventory(),
            PreparedCleanup::NewEpoch(prepared) => prepared.source_inventory(),
        }
    }

    pub(in super::super::super::super::super::super) fn proposed_cleanup_inventory(
        &self,
    ) -> PendingInventoryObservation {
        match &self.cleanup {
            PreparedCleanup::SameEpoch(prepared) => prepared.successor_inventory(),
            PreparedCleanup::NewEpoch(prepared) => prepared.successor_inventory(),
        }
    }

    pub(in super::super::super::super::super::super) fn finish(
        self,
    ) -> Result<ContextualPartAbandonmentReissue, PartAbandonmentReissueInspectionFailure> {
        let Self {
            cleanup, origin, ..
        } = self;
        match cleanup.finish() {
            Ok(cleanup) => Ok(ContextualPartAbandonmentReissue { origin, cleanup }),
            Err(failure) => Err(PartAbandonmentReissueInspectionFailure::from_cleanup(
                origin, failure,
            )),
        }
    }
}
