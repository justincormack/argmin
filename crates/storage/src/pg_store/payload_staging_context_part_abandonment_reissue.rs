// Copyright The Argmin Authors.
// SPDX-License-Identifier: Apache-2.0

//! Reissuing a cleanup must not discard its original finalizer or history.
//! A proposed position is not admission, accepted replacement, source retirement
//! or permission to delete. The authenticated carrier/custody switch is pending.

use super::super::{
    epoch_reissue::ContextualEpochReissue, ContextSubmissionObservation, ContextualReissue,
    ContextualReissuePreparationFailure, ContextualSubmission,
};
use super::{ContextualPartAbandonment, PendingInventoryObservation};
use crate::metadata_command::{MetadataCommandId, MetadataCommandReplicaState};
use crate::node_runtime::pg_store::payload_staging::command_context::ContextError;

#[path = "payload_staging_context_part_abandonment_reissue_preparation.rs"]
mod preparation;

#[cfg(test)]
pub(in super::super::super::super::super) use preparation::{
    PartAbandonmentReissueInspectionFailure, PreparedPartAbandonmentReissue,
};

/// The finalizer and diagnostics of the ORIGINAL capture are never replaced
/// by those of a reissued cleanup. This private holder has no split operation.
struct Origin {
    source: ContextualSubmission,
    prefix: MetadataCommandReplicaState,
    source_inventory: PendingInventoryObservation,
    cleanup_inventory: PendingInventoryObservation,
}

enum CleanupReissue {
    SameEpoch(ContextualReissue),
    NewEpoch(ContextualEpochReissue),
}

enum Kind {
    SameEpoch,
    NewEpoch,
}

/// Three inseparable complete commands: original finalizer, previous cleanup,
/// proposed cleanup. No Clone, encoding, split, borrowed holder, conversion to
/// a fresh submission, successor selection or cleanup Drop. Existing reissue
/// holders keep each cleanup's independent installation observation.
pub(in super::super::super::super::super) struct ContextualPartAbandonmentReissue {
    origin: Origin,
    cleanup: CleanupReissue,
}

/// Recover the ORIGINAL complete proposal, never just the rejected cleanup or
/// its bare context. The source's history and original observations stay owned.
pub(in super::super::super::super::super) struct PartAbandonmentReissueFailure {
    origin: Box<Origin>,
    failure: ContextualReissuePreparationFailure,
}

impl std::fmt::Debug for PartAbandonmentReissueFailure {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("PartAbandonmentReissueFailure")
            .field("error", self.failure.error())
            .field("source", &self.origin.source.observation())
            .finish_non_exhaustive()
    }
}

impl PartAbandonmentReissueFailure {
    pub(in super::super::super::super::super) fn error(&self) -> &ContextError {
        self.failure.error()
    }

    pub(in super::super::super::super::super) fn into_proposal(self) -> ContextualPartAbandonment {
        ContextualPartAbandonment {
            source: self.origin.source,
            cleanup: self.failure.into_submission(),
            prefix: self.origin.prefix,
            source_inventory: self.origin.source_inventory,
            cleanup_inventory: self.origin.cleanup_inventory,
        }
    }
}

impl ContextualPartAbandonment {
    /// A same-epoch log conflict retains BOTH cleanup candidates as well as the
    /// original finalizer. No immutable source, topology or cutoff is recaptured.
    pub(in super::super::super::super::super) fn into_same_epoch_reissue(
        self,
        id: MetadataCommandId,
    ) -> Result<ContextualPartAbandonmentReissue, PartAbandonmentReissueFailure> {
        derive(self, id, Kind::SameEpoch)
    }

    /// Historical recovery proposal only. A newer epoch may restart its log
    /// position at one; it does not establish current route/leader authority.
    pub(in super::super::super::super::super) fn into_epoch_reissue(
        self,
        id: MetadataCommandId,
    ) -> Result<ContextualPartAbandonmentReissue, PartAbandonmentReissueFailure> {
        derive(self, id, Kind::NewEpoch)
    }
}

fn derive(
    proposal: ContextualPartAbandonment,
    id: MetadataCommandId,
    kind: Kind,
) -> Result<ContextualPartAbandonmentReissue, PartAbandonmentReissueFailure> {
    let ContextualPartAbandonment {
        source,
        cleanup,
        prefix,
        source_inventory,
        cleanup_inventory,
    } = proposal;
    let origin = Origin {
        source,
        prefix,
        source_inventory,
        cleanup_inventory,
    };
    let cleanup = match kind {
        Kind::SameEpoch => cleanup
            .into_same_epoch_reissue(id)
            .map(CleanupReissue::SameEpoch),
        Kind::NewEpoch => cleanup.into_epoch_reissue(id).map(CleanupReissue::NewEpoch),
    };
    match cleanup {
        Ok(cleanup) => Ok(ContextualPartAbandonmentReissue { origin, cleanup }),
        Err(failure) => Err(PartAbandonmentReissueFailure {
            origin: Box::new(origin),
            failure,
        }),
    }
}

impl ContextualPartAbandonmentReissue {
    pub(in super::super::super::super::super) fn source(&self) -> ContextSubmissionObservation {
        self.origin.source.observation()
    }

    pub(in super::super::super::super::super) fn previous_cleanup(
        &self,
    ) -> ContextSubmissionObservation {
        match &self.cleanup {
            CleanupReissue::SameEpoch(pair) => pair.source(),
            CleanupReissue::NewEpoch(pair) => pair.source(),
        }
    }

    pub(in super::super::super::super::super) fn proposed_cleanup(
        &self,
    ) -> ContextSubmissionObservation {
        match &self.cleanup {
            CleanupReissue::SameEpoch(pair) => pair.successor(),
            CleanupReissue::NewEpoch(pair) => pair.successor(),
        }
    }

    /// Original preparation diagnostic, not a fresh/current-prefix observation
    /// or an inventory for the successor's newly proposed position.
    pub(in super::super::super::super::super) fn captured_prefix(
        &self,
    ) -> &MetadataCommandReplicaState {
        &self.origin.prefix
    }

    #[cfg(test)]
    pub(in super::super::super::super::super) fn assert_contexts_for_test(
        &self,
        source: &super::super::ContextualCommand,
        previous: &super::super::ContextualCommand,
        proposed: &super::super::ContextualCommand,
        topology: &crate::PgTopology,
    ) {
        self.origin.source.assert_context_for_test(source, topology);
        match &self.cleanup {
            CleanupReissue::SameEpoch(pair) => {
                pair.assert_contexts_for_test(previous, proposed, topology)
            }
            CleanupReissue::NewEpoch(pair) => {
                pair.assert_contexts_for_test(previous, proposed, topology)
            }
        }
    }
}
