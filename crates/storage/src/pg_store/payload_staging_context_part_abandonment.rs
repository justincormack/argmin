// Copyright The Argmin Authors.
// SPDX-License-Identifier: Apache-2.0

//! Storage-owned cleanup proposal derived from the original retained finalizer.
//! No source retirement, command installation or physical effects. Recovery
//! admission and authenticated carrier/custody activation remain separate gates.

use super::{ContextSubmissionObservation, ContextualSubmission};
use crate::metadata_command::{MetadataCommandId, MetadataCommandReplicaState};
use crate::node_runtime::pg_store::payload_staging::command_context::{
    ContextError, PendingInventoryObservation,
};
use crate::node_runtime::pg_store::PgStore;
use crate::PgTopology;

#[path = "payload_staging_context_part_abandonment_reissue.rs"]
mod reissue;

#[cfg(test)]
pub(in super::super::super::super) use reissue::{
    ContextualPartAbandonmentReissue, PartAbandonmentReissueFailure,
    PartAbandonmentReissueInspectionFailure, PreparedPartAbandonmentReissue,
};

/// Both candidates remain owned. No Clone, raw context, encoding, split,
/// successor selection or cleanup Drop. The original finalizer's history is
/// never reset; the new cleanup has never been sent by this holder.
pub(in super::super::super::super) struct ContextualPartAbandonment {
    source: ContextualSubmission,
    cleanup: ContextualSubmission,
    prefix: MetadataCommandReplicaState,
    source_inventory: PendingInventoryObservation,
    cleanup_inventory: PendingInventoryObservation,
}

pub(in super::super::super::super) struct PartAbandonmentPreparationFailure {
    source: Box<ContextualSubmission>,
    error: ContextError,
}

impl std::fmt::Debug for PartAbandonmentPreparationFailure {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("PartAbandonmentPreparationFailure")
            .field("error", &self.error)
            .field("source", &self.source.observation())
            .finish_non_exhaustive()
    }
}

impl PartAbandonmentPreparationFailure {
    pub(in super::super::super::super) fn error(&self) -> &ContextError {
        &self.error
    }

    pub(in super::super::super::super) fn into_submission(self) -> ContextualSubmission {
        *self.source
    }
}

impl ContextualSubmission {
    /// The node holds normal PG serialization. Capture the exact current
    /// closure, validate the original durable abandonment and inventory both
    /// candidates in ONE read snapshot. No network calls, clocks, allocation or
    /// writes. No unsent candidate escapes before snapshot finish succeeds.
    pub(in super::super::super::super) fn prepare_part_abandonment(
        self,
        pg: &PgStore,
        node_id: u32,
        topology: &PgTopology,
        id: MetadataCommandId,
    ) -> Result<ContextualPartAbandonment, PartAbandonmentPreparationFailure> {
        let prepare = || -> Result<_, ContextError> {
            pg.require_usable_connection()?;
            if self.context.id().pg_id().get() != pg.pg_id() {
                return Err(ContextError::CommandPg);
            }
            let snapshot = pg.command_build_snapshot()?;
            let prefix = pg.metadata_command_replica_state()?;
            let cleanup = self
                .context
                .capture_part_abandonment_in_open_transaction(pg, node_id, topology, id)?;
            let source_inventory = self
                .context
                .observe_pending_inventory_in_open_transaction(pg, topology)?;
            let cleanup_inventory =
                cleanup.observe_pending_inventory_in_open_transaction(pg, topology)?;
            snapshot.finish()?;
            Ok((cleanup, prefix, source_inventory, cleanup_inventory))
        };
        match prepare() {
            Ok((cleanup, prefix, source_inventory, cleanup_inventory)) => {
                Ok(ContextualPartAbandonment {
                    source: self,
                    cleanup: cleanup.into_submission(),
                    prefix,
                    source_inventory,
                    cleanup_inventory,
                })
            }
            Err(error) => Err(PartAbandonmentPreparationFailure {
                source: Box::new(self),
                error,
            }),
        }
    }
}

impl ContextualPartAbandonment {
    pub(in super::super::super::super) fn source(&self) -> ContextSubmissionObservation {
        self.source.observation()
    }

    pub(in super::super::super::super) fn cleanup(&self) -> ContextSubmissionObservation {
        self.cleanup.observation()
    }

    /// Observations of the completed snapshot, not cached apply/acceptance
    /// authority. Installation/application must rederive their own evidence.
    pub(in super::super::super::super) fn prefix(&self) -> &MetadataCommandReplicaState {
        &self.prefix
    }

    pub(in super::super::super::super) fn source_inventory(&self) -> PendingInventoryObservation {
        self.source_inventory
    }

    pub(in super::super::super::super) fn cleanup_inventory(&self) -> PendingInventoryObservation {
        self.cleanup_inventory
    }

    #[cfg(test)]
    pub(in super::super::super::super) fn assert_contexts_for_test(
        &self,
        source: &super::ContextualCommand,
        cleanup: &super::ContextualCommand,
        topology: &PgTopology,
    ) {
        self.source.assert_context_for_test(source, topology);
        self.cleanup.assert_context_for_test(cleanup, topology);
    }
}
