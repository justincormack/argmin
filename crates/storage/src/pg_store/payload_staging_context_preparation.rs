// Copyright The Argmin Authors.
// SPDX-License-Identifier: Apache-2.0

//! Pre-submission inspection of one complete context in one read-only PG view.
//! No network calls belong in this scope; the node keeps its PG serialization.
//! Ending the snapshot returns the original context, never an installable index
//! detached from it. Installation must derive its own indexes in the actual
//! pending transaction after the authenticated carrier is connected.

use super::{
    inventory, ContextError, ContextPendingInventory, ContextualCommand, Input,
    PendingInventoryObservation,
};
use crate::metadata_command::{MetadataCommandId, MetadataCommandReplicaState};
use crate::node_runtime::pg_store::{
    command_build_snapshot::MetadataCommandBuildSnapshot, PgStore,
};
use crate::PgTopology;

pub(in super::super::super) struct ContextPreparationFailure {
    context: Box<ContextualCommand>,
    error: ContextError,
}

impl std::fmt::Debug for ContextPreparationFailure {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("ContextPreparationFailure")
            .field("error", &self.error)
            .finish_non_exhaustive()
    }
}

impl ContextPreparationFailure {
    pub(in super::super::super) fn error(&self) -> &ContextError {
        &self.error
    }

    pub(in super::super::super) fn into_context(self) -> ContextualCommand {
        *self.context
    }
}

/// The snapshot is dropped first, restoring the connection on error/unwind.
/// No Clone, split, raw envelope/context projection or independently supplied
/// store on inspection/finish. Production inspection returns only scalar
/// diagnostics, never cloneable references, route/window records or page bytes.
pub(in super::super::super) struct PendingContextPreparation<'pg> {
    snapshot: MetadataCommandBuildSnapshot<'pg>,
    context: ContextualCommand,
    prefix: MetadataCommandReplicaState,
    inventory: ContextPendingInventory,
}

impl PendingContextPreparation<'_> {
    pub(in super::super::super) fn id(&self) -> MetadataCommandId {
        self.context.id()
    }

    /// Observed prefix only, not recovery authorization or current custody.
    pub(in super::super::super) fn prefix(&self) -> &MetadataCommandReplicaState {
        &self.prefix
    }

    pub(in super::super::super) fn inventory(&self) -> PendingInventoryObservation {
        self.inventory.observation()
    }

    #[cfg(test)]
    pub(in super::super::super) fn inventory_for_test(&self) -> &ContextPendingInventory {
        &self.inventory
    }

    /// Roll back the read snapshot before releasing the complete input. Never
    /// commit or export cached indexes as authority for a later transaction.
    pub(in super::super::super) fn finish(
        self,
    ) -> Result<ContextualCommand, ContextPreparationFailure> {
        let Self {
            snapshot,
            context,
            prefix: _,
            inventory: _,
        } = self;
        match snapshot.finish() {
            Ok(()) => Ok(context),
            Err(error) => Err(ContextPreparationFailure {
                context: Box::new(context),
                error: error.into(),
            }),
        }
    }
}

pub(super) fn prepare<'pg>(
    context: ContextualCommand,
    pg: &'pg PgStore,
    topology: &PgTopology,
) -> Result<PendingContextPreparation<'pg>, ContextPreparationFailure> {
    inspect(context, pg, topology, Inspection::Inventory)
}

pub(super) fn prepare_abandoned_part<'pg>(
    context: ContextualCommand,
    pg: &'pg PgStore,
    node_id: u32,
    topology: &PgTopology,
) -> Result<PendingContextPreparation<'pg>, ContextPreparationFailure> {
    inspect(
        context,
        pg,
        topology,
        Inspection::AbandonedPartSource(node_id),
    )
}

enum Inspection {
    Inventory,
    AbandonedPartSource(u32),
}

fn inspect<'pg>(
    context: ContextualCommand,
    pg: &'pg PgStore,
    topology: &PgTopology,
    inspection: Inspection,
) -> Result<PendingContextPreparation<'pg>, ContextPreparationFailure> {
    let inspect = || -> Result<_, ContextError> {
        pg.require_usable_connection()?;
        // Reject a caller-selected ordinary/other context before storage reads.
        let source = match inspection {
            Inspection::Inventory => None,
            Inspection::AbandonedPartSource(node_id) => {
                let Input::AbandonedPart(candidate) = &context.input else {
                    return Err(ContextError::CommandKind);
                };
                Some((candidate.bind(topology)?, node_id))
            }
        };
        let snapshot = pg.command_build_snapshot()?;
        // Pin the deferred SQLite view even for an empty-payload command,
        // before projecting requirements that may read session encryption.
        let prefix = pg.metadata_command_replica_state()?;
        if let Some((bound, node_id)) = source {
            bound.require_abandoned_source_in_open_transaction(pg, node_id)?;
        }
        let inventory = inventory::derive(&context, pg, topology)?;
        Ok((snapshot, prefix, inventory))
    };
    match inspect() {
        Ok((snapshot, prefix, inventory)) => Ok(PendingContextPreparation {
            snapshot,
            context,
            prefix,
            inventory,
        }),
        Err(error) => Err(ContextPreparationFailure {
            context: Box::new(context),
            error,
        }),
    }
}
