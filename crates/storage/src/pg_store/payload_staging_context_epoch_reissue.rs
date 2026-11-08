// Copyright The Argmin Authors.
// SPDX-License-Identifier: Apache-2.0

//! Retain an uncertain/accepted source while describing a successor in a NEW
//! epoch. This is not ordinary slot replacement or recovery authorization.
//! Actual recovery still requires the authoritative listing/leader proof,
//! authenticated carrier, durable source validation and current route authority.

use super::{
    ContextSubmissionObservation, ContextualReissuePreparationFailure, ContextualSubmission,
    InstallationObservation,
};
use crate::metadata_command::{MetadataCommandId, MetadataCommandReplicaState};
use crate::node_runtime::pg_store::command_build_snapshot::MetadataCommandBuildSnapshot;
use crate::node_runtime::pg_store::payload_staging::command_context::{
    ContextError, PendingInventoryObservation,
};
use crate::node_runtime::pg_store::PgStore;
use crate::PgTopology;

enum Inspection {
    Inventory,
    AbandonedPartSource(u32),
}

/// No Clone, encoding, split, ordinary-reissue conversion, successor extraction
/// or cleanup Drop. Until recovery acceptance is connected, neither command may
/// be selected or its original observation discarded. A target ID is a proposal,
/// not proof that this process is authorized in that epoch.
pub(in super::super::super::super) struct ContextualEpochReissue {
    source: ContextualSubmission,
    successor: ContextualSubmission,
}

/// Own the original pair on every inspection/finish failure. No bare context
/// or fresh-submission conversion can erase the source's installation history.
pub(in super::super::super::super) struct EpochReissueInspectionFailure {
    reissue: Box<ContextualEpochReissue>,
    error: ContextError,
}

impl std::fmt::Debug for EpochReissueInspectionFailure {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("EpochReissueInspectionFailure")
            .field("error", &self.error)
            .finish_non_exhaustive()
    }
}

impl EpochReissueInspectionFailure {
    // Internal composition retains the entire owning pair on failure.
    pub(super) fn into_parts(self) -> (ContextualEpochReissue, ContextError) {
        (*self.reissue, self.error)
    }

    pub(in super::super::super::super) fn error(&self) -> &ContextError {
        &self.error
    }

    pub(in super::super::super::super) fn into_reissue(self) -> ContextualEpochReissue {
        *self.reissue
    }
}

/// Snapshot drops before the owning pair. Only scalar observations escape;
/// finish returns the entire proposal, never installable pages or a certificate.
pub(in super::super::super::super) struct PreparedEpochReissue<'pg> {
    snapshot: MetadataCommandBuildSnapshot<'pg>,
    reissue: ContextualEpochReissue,
    prefix: MetadataCommandReplicaState,
    source_inventory: PendingInventoryObservation,
    successor_inventory: PendingInventoryObservation,
}

impl ContextualSubmission {
    /// Consume one complete holder, never a borrowed context or reconstructed
    /// bare command. Preserve all immutable context, including the original
    /// finalizer of a cleanup derivative. Epoch-reset log index 1 is valid;
    /// changing PG, staying in the same epoch or moving backwards is not.
    pub(in super::super::super::super) fn into_epoch_reissue(
        self,
        id: MetadataCommandId,
    ) -> Result<ContextualEpochReissue, ContextualReissuePreparationFailure> {
        match self.context.into_epoch_reissue_pair(id) {
            Ok((source, successor)) => Ok(ContextualEpochReissue {
                source: Self {
                    context: source,
                    installation: self.installation,
                },
                successor: Self {
                    context: successor,
                    installation: InstallationObservation::NoAcceptanceObserved,
                },
            }),
            Err(failure) => Err(ContextualReissuePreparationFailure {
                failure,
                installation: self.installation,
            }),
        }
    }
}

impl ContextualEpochReissue {
    /// The node holds its normal PG serialization. This inventories protection
    /// in one coherent view; the observed prefix need not be the proposed epoch
    /// and does not establish current route/recovery authority.
    pub(in super::super::super::super) fn prepare_pending<'pg>(
        self,
        pg: &'pg PgStore,
        topology: &PgTopology,
    ) -> Result<PreparedEpochReissue<'pg>, EpochReissueInspectionFailure> {
        self.inspect(pg, topology, Inspection::Inventory)
    }

    /// Additionally authenticate the original finalizer for BOTH cleanup
    /// candidates in this same view. Apply must repeat that check transactionally.
    pub(in super::super::super::super) fn prepare_abandoned_part_pending<'pg>(
        self,
        pg: &'pg PgStore,
        node_id: u32,
        topology: &PgTopology,
    ) -> Result<PreparedEpochReissue<'pg>, EpochReissueInspectionFailure> {
        self.inspect(pg, topology, Inspection::AbandonedPartSource(node_id))
    }

    fn inspect<'pg>(
        self,
        pg: &'pg PgStore,
        topology: &PgTopology,
        inspection: Inspection,
    ) -> Result<PreparedEpochReissue<'pg>, EpochReissueInspectionFailure> {
        let inspect = || -> Result<_, ContextError> {
            pg.require_usable_connection()?;
            self.successor
                .context
                .require_epoch_reissue_of(&self.source.context, topology)?;
            if self.source.context.id().pg_id().get() != pg.pg_id() {
                return Err(ContextError::CommandPg);
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
                Ok(PreparedEpochReissue {
                    snapshot,
                    reissue: self,
                    prefix,
                    source_inventory,
                    successor_inventory,
                })
            }
            Err(error) => Err(EpochReissueInspectionFailure {
                reissue: Box::new(self),
                error,
            }),
        }
    }

    pub(in super::super::super::super) fn source(&self) -> ContextSubmissionObservation {
        self.source.observation()
    }

    pub(in super::super::super::super) fn successor(&self) -> ContextSubmissionObservation {
        self.successor.observation()
    }

    #[cfg(test)]
    pub(in super::super::super::super) fn assert_contexts_for_test(
        &self,
        source: &super::ContextualCommand,
        successor: &super::ContextualCommand,
        topology: &crate::PgTopology,
    ) {
        self.source.assert_context_for_test(source, topology);
        self.successor.assert_context_for_test(successor, topology);
    }
}

impl PreparedEpochReissue<'_> {
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
    ) -> Result<ContextualEpochReissue, EpochReissueInspectionFailure> {
        let Self {
            snapshot, reissue, ..
        } = self;
        match snapshot.finish() {
            Ok(()) => Ok(reissue),
            Err(error) => Err(EpochReissueInspectionFailure {
                reissue: Box::new(reissue),
                error: error.into(),
            }),
        }
    }
}
