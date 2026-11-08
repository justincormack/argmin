// Copyright The Argmin Authors.
// SPDX-License-Identifier: Apache-2.0

//! Inspect both possible commands in one read-only PG snapshot without
//! detaching either context or installation history. This is not replacement,
//! command acceptance, recovery admission or a current-custody certificate.

use super::{ContextSubmissionObservation, ContextualReissue};
use crate::metadata_command::MetadataCommandReplicaState;
use crate::node_runtime::pg_store::command_build_snapshot::MetadataCommandBuildSnapshot;
use crate::node_runtime::pg_store::payload_staging::command_context::{
    ContextError, PendingContextMatch, PendingInventoryObservation,
};
use crate::node_runtime::pg_store::PgStore;
use crate::PgTopology;

enum Inspection {
    Inventory,
    AbandonedPartSource(u32),
}

pub(in super::super::super::super) struct ContextualReissueInspectionFailure {
    reissue: Box<ContextualReissue>,
    error: ContextError,
}

impl std::fmt::Debug for ContextualReissueInspectionFailure {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("ContextualReissueInspectionFailure")
            .field("error", &self.error)
            .finish_non_exhaustive()
    }
}

impl ContextualReissueInspectionFailure {
    // Composition inside the owning submission module only. The pair remains
    // inseparable; this never exposes either command or resets its history.
    pub(super) fn into_parts(self) -> (ContextualReissue, ContextError) {
        (*self.reissue, self.error)
    }

    pub(in super::super::super::super) fn error(&self) -> &ContextError {
        &self.error
    }

    pub(in super::super::super::super) fn into_reissue(self) -> ContextualReissue {
        *self.reissue
    }
}

/// Field order releases the read snapshot before dropping both contexts. Only
/// scalar diagnostics leave this scope; neither source nor successor may be
/// selected, extracted or sent while its snapshot is held. No network calls.
pub(in super::super::super::super) struct PreparedContextualReissue<'pg> {
    snapshot: MetadataCommandBuildSnapshot<'pg>,
    reissue: ContextualReissue,
    prefix: MetadataCommandReplicaState,
    source_inventory: PendingInventoryObservation,
    successor_inventory: PendingInventoryObservation,
}

impl ContextualReissue {
    /// The node must hold its normal PG serialization. Derive both inventories
    /// in one pinned read view, not two nested or sequential snapshot scopes.
    pub(in super::super::super::super) fn prepare_pending<'pg>(
        self,
        pg: &'pg PgStore,
        topology: &PgTopology,
    ) -> Result<PreparedContextualReissue<'pg>, ContextualReissueInspectionFailure> {
        self.inspect(pg, topology, Inspection::Inventory)
    }

    /// Verify the original abandoned finalizer for BOTH possible follow-ups in
    /// the same view as their inventories. Generic protection inspection remains
    /// separate: an invalid command may still have payload needing protection.
    /// This does not authorize replacement or forget either installation history.
    pub(in super::super::super::super) fn prepare_abandoned_part_pending<'pg>(
        self,
        pg: &'pg PgStore,
        node_id: u32,
        topology: &PgTopology,
    ) -> Result<PreparedContextualReissue<'pg>, ContextualReissueInspectionFailure> {
        self.inspect(pg, topology, Inspection::AbandonedPartSource(node_id))
    }

    fn inspect<'pg>(
        self,
        pg: &'pg PgStore,
        topology: &PgTopology,
        inspection: Inspection,
    ) -> Result<PreparedContextualReissue<'pg>, ContextualReissueInspectionFailure> {
        let inspect = || -> Result<_, ContextError> {
            pg.require_usable_connection()?;
            if self
                .successor
                .context
                .pending_match(&self.source.context, topology)?
                != PendingContextMatch::SameEpochReissue
            {
                return Err(ContextError::ReissueContext);
            }
            let snapshot = pg.command_build_snapshot()?;
            let prefix = pg.metadata_command_replica_state()?;
            let inventory = |context: &super::ContextualCommand| match inspection {
                Inspection::Inventory => {
                    context.observe_pending_inventory_in_open_transaction(pg, topology)
                }
                Inspection::AbandonedPartSource(node_id) => context
                    .observe_abandoned_part_inventory_in_open_transaction(pg, node_id, topology),
            };
            let source_inventory = inventory(&self.source.context)?;
            let successor_inventory = inventory(&self.successor.context)?;
            Ok((snapshot, prefix, source_inventory, successor_inventory))
        };
        match inspect() {
            Ok((snapshot, prefix, source_inventory, successor_inventory)) => {
                Ok(PreparedContextualReissue {
                    snapshot,
                    reissue: self,
                    prefix,
                    source_inventory,
                    successor_inventory,
                })
            }
            Err(error) => Err(ContextualReissueInspectionFailure {
                reissue: Box::new(self),
                error,
            }),
        }
    }
}

impl PreparedContextualReissue<'_> {
    pub(in super::super::super::super) fn source(&self) -> ContextSubmissionObservation {
        self.reissue.source()
    }

    pub(in super::super::super::super) fn successor(&self) -> ContextSubmissionObservation {
        self.reissue.successor()
    }

    pub(in super::super::super::super) fn prefix(&self) -> &MetadataCommandReplicaState {
        &self.prefix
    }

    pub(in super::super::super::super) fn source_inventory(&self) -> PendingInventoryObservation {
        self.source_inventory
    }

    pub(in super::super::super::super) fn successor_inventory(
        &self,
    ) -> PendingInventoryObservation {
        self.successor_inventory
    }

    pub(in super::super::super::super) fn finish(
        self,
    ) -> Result<ContextualReissue, ContextualReissueInspectionFailure> {
        let Self {
            snapshot, reissue, ..
        } = self;
        match snapshot.finish() {
            Ok(()) => Ok(reissue),
            Err(error) => Err(ContextualReissueInspectionFailure {
                reissue: Box::new(reissue),
                error: error.into(),
            }),
        }
    }
}
