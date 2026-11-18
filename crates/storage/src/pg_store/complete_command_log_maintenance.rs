// Copyright The Argmin Authors.
// SPDX-License-Identifier: Apache-2.0

//! Complete-prefix maintenance for coordinated integration. Explicit complete
//! grammar only; live node/RPC/startup dispatch is still current-only. No
//! pending slot is removed, and no owner, session or reference is transferred.

use super::*;
use crate::node_runtime::server::{CompleteLogMutation, CompleteLogMutationAuthority};
use crate::PgTopology;

#[cfg(test)]
thread_local! {
    static BEFORE_COMPLETE_MAINTENANCE_EFFECT: std::cell::RefCell<Option<Box<dyn FnOnce()>>> =
        const { std::cell::RefCell::new(None) };
}

// The shared V13 implementation remains explicit. Complete compaction cannot
// select its grammar without admitted mutation authority in production.
pub(super) enum LogCompaction<'a> {
    Current,
    Complete(&'a PgTopology, &'a CompleteLogMutationAuthority<'a>),
    #[cfg(test)]
    Fixture(&'a PgTopology),
}

impl LogCompaction<'_> {
    pub(super) fn grammar(&self) -> TerminalLogGrammar<'_> {
        match self {
            Self::Current => TerminalLogGrammar::Current,
            Self::Complete(topology, _) => TerminalLogGrammar::Complete(topology),
            #[cfg(test)]
            Self::Fixture(topology) => TerminalLogGrammar::Complete(topology),
        }
    }

    pub(super) fn require_effect(
        &self,
        pg: &PgStore,
        epoch: ClusterEpoch,
    ) -> Result<(), StoreError> {
        match self {
            Self::Complete(_, authority) => {
                authority.require_effect(pg, epoch, CompleteLogMutation::Compact)
            }
            Self::Current => Ok(()),
            #[cfg(test)]
            Self::Fixture(_) => Ok(()),
        }
    }
}

pub(super) enum CheckpointRecording<'a> {
    Current,
    Complete(&'a CompleteLogMutationAuthority<'a>),
}

impl CheckpointRecording<'_> {
    pub(super) fn require_effect(
        &self,
        pg: &PgStore,
        epoch: ClusterEpoch,
    ) -> Result<(), StoreError> {
        match self {
            Self::Current => Ok(()),
            Self::Complete(authority) => {
                authority.require_effect(pg, epoch, CompleteLogMutation::Record)
            }
        }
    }
}

#[derive(Debug, thiserror::Error)]
pub(in crate::node_runtime) enum CompleteMaintenanceError {
    #[error(transparent)]
    Store(#[from] StoreError),
    #[error(transparent)]
    Snapshot(#[from] MetadataError),
}

// Maintenance errors confer no absence/custody evidence. Retire a connection
// left in a transaction (including on unwind), so later commands cannot adopt
// writes whose rollback was not established. Reopen establishes durable state.
struct MaintenanceBoundary<'a>(&'a PgStore);

impl Drop for MaintenanceBoundary<'_> {
    fn drop(&mut self) {
        if !self.0.conn.is_autocommit() {
            self.0.retire_connection();
        }
    }
}

/// Exact request scope for installed maintenance. This selects a grammar, not
/// route authority: the node must still admit inspection or primary mutation.
pub(in crate::node_runtime) struct CompleteLogMaintenanceRequest<'a> {
    node_id: u32,
    pg_id: PgId,
    epoch: ClusterEpoch,
    topology: &'a PgTopology,
}

impl<'a> CompleteLogMaintenanceRequest<'a> {
    pub(in crate::node_runtime) fn new(
        node_id: NodeId,
        pg_id: PgId,
        epoch: ClusterEpoch,
        topology: &'a PgTopology,
    ) -> Result<Self, StoreError> {
        if !topology.contains_pg(pg_id) || i64::try_from(epoch.get()).is_err() {
            return Err(StoreError::RouteCapabilitySubjectMismatch {
                operation: "complete maintenance request topology/epoch",
            });
        }
        Ok(Self {
            node_id: node_id.as_u32(),
            pg_id,
            epoch,
            topology,
        })
    }

    fn require_store(&self, pg: &PgStore) -> Result<(), StoreError> {
        if pg.pg_id != self.pg_id.get() {
            return Err(StoreError::RouteCapabilitySubjectMismatch {
                operation: "complete maintenance store PG",
            });
        }
        pg.require_complete_maintenance_boundary(self.topology)
    }

    pub(in crate::node_runtime) fn checkpoint(
        &self,
        pg: &PgStore,
    ) -> Result<MetadataCommandCheckpoint, CompleteMaintenanceError> {
        self.require_store(pg)?;
        pg.capture_complete_metadata_command_checkpoint(
            self.node_id,
            self.topology,
            Some(self.epoch),
        )
    }

    pub(in crate::node_runtime) fn record_checkpoint(
        &self,
        pg: &PgStore,
        authority: &CompleteLogMutationAuthority<'_>,
    ) -> Result<MetadataCommandCheckpoint, CompleteMaintenanceError> {
        authority.require_request(
            pg,
            self.node_id,
            self.pg_id,
            self.epoch,
            self.topology,
            CompleteLogMutation::Record,
        )?;
        let checkpoint = self.checkpoint(pg)?;
        let _boundary = MaintenanceBoundary(pg);
        pg.record_metadata_command_checkpoint_authorized(
            &checkpoint,
            CheckpointRecording::Complete(authority),
        )?;
        Ok(checkpoint)
    }

    pub(in crate::node_runtime) fn compact(
        &self,
        pg: &PgStore,
        authority: &CompleteLogMutationAuthority<'_>,
    ) -> Result<MetadataCommandLogCompactionStatus, CompleteMaintenanceError> {
        authority.require_request(
            pg,
            self.node_id,
            self.pg_id,
            self.epoch,
            self.topology,
            CompleteLogMutation::Compact,
        )?;
        self.require_store(pg)?;
        Ok(pg.compact_complete_metadata_command_log_inner(
            self.topology,
            METADATA_COMMAND_TERMINAL_RECEIPT_RETAIN_PER_EPOCH,
            Some(self.epoch),
            LogCompaction::Complete(self.topology, authority),
        )?)
    }
}

impl PgStore {
    #[cfg(test)]
    pub(super) fn test_before_complete_maintenance_effect(&self) {
        if let Some(hook) = BEFORE_COMPLETE_MAINTENANCE_EFFECT.with(|slot| slot.borrow_mut().take())
        {
            hook();
        }
    }

    #[cfg(test)]
    pub(in crate::node_runtime) fn test_with_before_complete_maintenance_effect<T>(
        &self,
        hook: impl FnOnce() + 'static,
        action: impl FnOnce() -> T,
    ) -> T {
        struct Reset(Option<Box<dyn FnOnce()>>);
        impl Drop for Reset {
            fn drop(&mut self) {
                BEFORE_COMPLETE_MAINTENANCE_EFFECT.with(|slot| *slot.borrow_mut() = self.0.take());
            }
        }
        let _reset = Reset(
            BEFORE_COMPLETE_MAINTENANCE_EFFECT.with(|slot| slot.replace(Some(Box::new(hook)))),
        );
        action()
    }

    fn require_complete_maintenance_boundary(
        &self,
        topology: &PgTopology,
    ) -> Result<(), StoreError> {
        self.require_usable_connection()?;
        if !self.conn.is_autocommit() || !topology.contains_pg(PgId::new(self.pg_id)) {
            return Err(StoreError::RouteCapabilitySubjectMismatch {
                operation: "complete log maintenance transaction/topology",
            });
        }
        Ok(())
    }

    /// Capture the store's own epoch/prefix and tables in one read-only view.
    /// Requires a settled prefix, full digest agreement, and no pending custody
    /// of ANY epoch. This is not startup replay or acting-set convergence.
    pub(in super::super) fn complete_metadata_command_checkpoint(
        &self,
        node_id: u32,
        topology: &PgTopology,
    ) -> Result<MetadataCommandCheckpoint, CompleteMaintenanceError> {
        self.capture_complete_metadata_command_checkpoint(node_id, topology, None)
    }

    fn capture_complete_metadata_command_checkpoint(
        &self,
        node_id: u32,
        topology: &PgTopology,
        requested_epoch: Option<ClusterEpoch>,
    ) -> Result<MetadataCommandCheckpoint, CompleteMaintenanceError> {
        self.require_complete_maintenance_boundary(topology)?;
        let _boundary = MaintenanceBoundary(self);
        let snapshot = self.command_build_snapshot()?;
        // Requested epoch is validated by the shared prefix checker inside
        // this snapshot, never silently replaced with the store's newer epoch.
        let epoch = match requested_epoch {
            Some(epoch) => epoch,
            None => self.metadata_command_replica_state()?.cluster_epoch,
        };
        let checkpoint = self.metadata_command_checkpoint_with_grammar(
            node_id,
            epoch,
            TerminalLogGrammar::Complete(topology),
        )?;
        snapshot.finish()?;
        Ok(checkpoint)
    }

    #[cfg(test)]
    pub(in super::super) fn record_complete_metadata_command_checkpoint(
        &self,
        node_id: u32,
        topology: &PgTopology,
    ) -> Result<MetadataCommandCheckpoint, CompleteMaintenanceError> {
        let checkpoint = self.complete_metadata_command_checkpoint(node_id, topology)?;
        let _boundary = MaintenanceBoundary(self);
        self.record_metadata_command_checkpoint(&checkpoint)?;
        Ok(checkpoint)
    }

    #[cfg(test)]
    pub(in super::super) fn compact_complete_metadata_command_log(
        &self,
        topology: &PgTopology,
    ) -> Result<MetadataCommandLogCompactionStatus, StoreError> {
        self.compact_complete_metadata_command_log_inner(
            topology,
            METADATA_COMMAND_TERMINAL_RECEIPT_RETAIN_PER_EPOCH,
            None,
            LogCompaction::Fixture(topology),
        )
    }

    #[cfg(test)]
    pub(in super::super) fn test_compact_complete_metadata_command_log_with_retention(
        &self,
        topology: &PgTopology,
        receipt_retention: u64,
    ) -> Result<MetadataCommandLogCompactionStatus, StoreError> {
        self.compact_complete_metadata_command_log_inner(
            topology,
            receipt_retention,
            None,
            LogCompaction::Fixture(topology),
        )
    }

    fn compact_complete_metadata_command_log_inner(
        &self,
        topology: &PgTopology,
        receipt_retention: u64,
        requested_epoch: Option<ClusterEpoch>,
        compaction: LogCompaction<'_>,
    ) -> Result<MetadataCommandLogCompactionStatus, StoreError> {
        self.require_complete_maintenance_boundary(topology)?;
        let epoch = self.metadata_command_replica_state()?.cluster_epoch;
        if let Some(requested) = requested_epoch {
            if requested != epoch {
                return Err(StoreError::StaleMetadataCommand {
                    node_id: 0,
                    pg_id: self.pg_id,
                    command_epoch: epoch,
                    current_epoch: requested,
                });
            }
        }
        let grammar = TerminalLogGrammar::Complete(topology);
        // Check custody before NoCheckpoint as well. Even an older complete
        // slot must remain visible to future startup/recovery admission.
        if grammar.has_pending(self, 0, epoch)? {
            return Ok(MetadataCommandLogCompactionStatus::PendingCommand {
                retained_entries: self.metadata_command_log_stats(epoch)?.retained_entries,
            });
        }
        let _boundary = MaintenanceBoundary(self);
        self.compact_metadata_command_log_with_grammar(epoch, receipt_retention, compaction)
    }

    /// Adapter for owner-local component tests, not an alternate
    /// maintenance implementation. Only the receipt-retention bound is injected.
    #[cfg(test)]
    pub(in super::super) fn test_checkpoint_and_compact_complete_command(
        &self,
        node_id: u32,
        command: &payload_staging::EncodedContextualCommand<'_>,
        receipt_retention: u64,
    ) -> Result<MetadataCommandCheckpoint, CompleteMaintenanceError> {
        if command.id().pg_id().get() != self.pg_id {
            return Err(TerminalCommandIdentity::complete(command)
                .conflict(node_id, self.pg_id)
                .into());
        }
        let state = self.metadata_command_replica_state()?;
        if state.cluster_epoch != command.id().cluster_epoch() {
            return Err(StoreError::StaleMetadataCommand {
                node_id,
                pg_id: self.pg_id,
                command_epoch: command.id().cluster_epoch(),
                current_epoch: state.cluster_epoch,
            }
            .into());
        }
        let checkpoint =
            self.record_complete_metadata_command_checkpoint(node_id, command.topology())?;
        self.test_compact_complete_metadata_command_log_with_retention(
            command.topology(),
            receipt_retention,
        )?;
        Ok(checkpoint)
    }
}
