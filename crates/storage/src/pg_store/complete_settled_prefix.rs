// Copyright The Argmin Authors.
// SPDX-License-Identifier: Apache-2.0

//! Bounded LOCAL settled-prefix observation, not primary/witness confirmation.
//! Complete startup recovery or explicit read-only Direct PUT reinspection can
//! seed the private validation entry. Closed
//! same-epoch complete application can advance an already validated entry, and
//! closed pending installation/publication/retirement and confirmed stream
//! cursor allocation can preserve it. Other
//! SQLite writes invalidate it, including noncanonical tables and rolled-back
//! writes. Lookup never repairs caches, replays commands, scans
//! materialized tables, generates checkpoints, or promotes a cold entry.
//!
//! The candidate embedded confirmation owner holds primary serialization and
//! compares the designated witness's prefix while capturing the exact window.
//! Unix/TLS use the same prefix contract. The consuming physical grant remains
//! unimplemented; confirmed embedded stream allocation is not that grant.
//! This read scope and its scalar prefix are not physical-effect authority.

use super::*;
use crate::node_runtime::pg_store::command_build_snapshot::MetadataCommandBuildSnapshot;
use crate::PgTopology;
use std::cell::RefCell;

// Only these closed transaction implementations can seed/advance validation.
// No sibling can "mark clean" using an arbitrary replica-state DTO or callback.
#[path = "complete_command_application.rs"]
mod application;
#[path = "complete_command_recovery.rs"]
mod recovery;
#[path = "complete_prefix_transitions.rs"]
mod transitions;
pub(in crate::node_runtime) use application::{
    CompleteApplicationError, CompleteApplicationQuery, CompleteApplicationRequest,
};
pub(super) use transitions::{PrefixTransition, PublicationAuthority, RetirementEffectAuthority};

#[derive(Clone, Copy, PartialEq, Eq)]
struct Stamp {
    // Independent of the cache itself, and stable if the PgStore is moved.
    // Reopen/import gets another identity; failed replacement retires it.
    lifetime: [u8; 16],
    local_changes: u64,
    external_changes: i64,
    schema_version: i64,
}

impl Stamp {
    fn read(pg: &PgStore) -> Result<Self, StoreError> {
        pg.require_usable_connection()?;
        Ok(Self {
            lifetime: pg.allocation_lifetime.identity(pg.pg_id)?,
            local_changes: pg.conn.total_changes(),
            external_changes: pg.query_row_cached(
                "PRAGMA main.data_version",
                [],
                "read complete prefix external change stamp",
                |row| row.get(0),
            )?,
            schema_version: pg.query_row_cached(
                "PRAGMA main.schema_version",
                [],
                "read complete prefix schema stamp",
                |row| row.get(0),
            )?,
        })
    }
}

struct ValidatedPrefix {
    topology: PgTopology,
    pg_id: u32,
    state: MetadataCommandReplicaState,
    stamp: Stamp,
}

#[derive(Default)]
pub(in crate::node_runtime::pg_store) struct CompletePrefixValidationCache {
    entry: RefCell<Option<ValidatedPrefix>>,
}

impl CompletePrefixValidationCache {
    pub(in crate::node_runtime::pg_store) fn invalidate(&self) {
        self.entry.borrow_mut().take();
    }

    // Called only after a closed validation/preservation transaction commits. A
    // writer between validation and here must not be blessed by a fresh stamp.
    fn retain_committed(&self, pg: &PgStore, entry: ValidatedPrefix) -> Result<(), StoreError> {
        self.invalidate();
        if pg.conn.is_autocommit() && Stamp::read(pg)? == entry.stamp {
            *self.entry.borrow_mut() = Some(entry);
        }
        Ok(())
    }

    fn require(
        &self,
        pg: &PgStore,
        topology: &PgTopology,
        state: &MetadataCommandReplicaState,
    ) -> Result<(), CompleteSettledPrefixError> {
        let stamp = Stamp::read(pg)?;
        let mut entry = self.entry.borrow_mut();
        let Some(validated) = entry.as_ref() else {
            return Err(CompleteSettledPrefixError::ValidationRequired);
        };
        if validated.stamp != stamp || validated.pg_id != pg.pg_id || validated.state != *state {
            entry.take();
            return Err(CompleteSettledPrefixError::ValidationRequired);
        }
        if validated.topology != *topology {
            return Err(StoreError::RouteCapabilitySubjectMismatch {
                operation: "complete settled prefix validated topology",
            }
            .into());
        }
        Ok(())
    }
}

#[derive(Debug, thiserror::Error)]
pub(in crate::node_runtime) enum CompleteSettledPrefixError {
    #[error("complete settled prefix requires explicit local validation")]
    ValidationRequired,
    #[error(transparent)]
    Store(#[from] StoreError),
    #[error(transparent)]
    Snapshot(#[from] MetadataError),
}

/// Own one read-only SQLite snapshot, never a transferable positive grant.
/// Holding this scope prevents local SQL writes; callers must also retain the
/// owning PG mutex. Prefix fields alone establish neither primary authority nor
/// matching witness evidence. No mutation API accepts this scope or its DTO.
pub(in crate::node_runtime) struct CompleteSettledPrefixRead<'pg> {
    snapshot: MetadataCommandBuildSnapshot<'pg>,
    state: MetadataCommandReplicaState,
}

impl CompleteSettledPrefixRead<'_> {
    pub(in crate::node_runtime) fn prefix(&self) -> &MetadataCommandReplicaState {
        &self.state
    }

    pub(in crate::node_runtime) fn finish(self) -> Result<(), CompleteSettledPrefixError> {
        self.snapshot.finish()?;
        Ok(())
    }
}

impl PgStore {
    // A settled local prefix cannot coexist with a later durable command or
    // terminal anchor, whether in this epoch or a future one. Use the ordered
    // primary-key boundary, not epoch-scoped counts or payload decoding. Each
    // query is one indexed seek and at most one scalar row. Full startup
    // validation separately rejects foreign-PG carriers before minting the
    // cache; these bounded lookups explicitly select this store's PG. Historical
    // evidence remains valid and is neither removed nor treated as a new tail.
    fn require_complete_prefix_evidence_bound(
        &self,
        node_id: u32,
        state: &MetadataCommandReplicaState,
    ) -> Result<(), StoreError> {
        for sql in [
            "SELECT cluster_epoch, log_index FROM metadata_command_log \
             WHERE pg_id = ?2 AND (cluster_epoch, pg_id, log_index) > (?1, ?2, ?3) \
             ORDER BY cluster_epoch, pg_id, log_index LIMIT 1",
            "SELECT cluster_epoch, log_index FROM metadata_command_terminal_receipts \
             WHERE pg_id = ?2 AND (cluster_epoch, pg_id, log_index) > (?1, ?2, ?3) \
             ORDER BY cluster_epoch, pg_id, log_index LIMIT 1",
            "SELECT cluster_epoch, applied_log_index FROM metadata_command_checkpoints \
             WHERE pg_id = ?2 AND (cluster_epoch, pg_id, applied_log_index) > (?1, ?2, ?3) \
             ORDER BY cluster_epoch, pg_id, applied_log_index LIMIT 1",
        ] {
            if let Some((epoch, log_index)) = self.query_row_cached_optional(
                sql,
                params![
                    state.cluster_epoch.get() as i64,
                    self.pg_id,
                    state.applied_log_index as i64
                ],
                "inspect complete prefix for later durable evidence",
                |row| Ok((row.get::<_, u64>(0)?, row.get::<_, u64>(1)?)),
            )? {
                let cluster_epoch =
                    ClusterEpoch::new(epoch).ok_or(StoreError::MetadataCommandLogConflict {
                        node_id,
                        pg_id: self.pg_id,
                        cluster_epoch: state.cluster_epoch,
                        log_index,
                    })?;
                return Err(StoreError::MetadataCommandLogConflict {
                    node_id,
                    pg_id: self.pg_id,
                    cluster_epoch,
                    log_index,
                });
            }
        }
        Ok(())
    }

    pub(in crate::node_runtime) fn inspect_complete_settled_prefix(
        &self,
        node_id: u32,
        epoch: ClusterEpoch,
        topology: &PgTopology,
    ) -> Result<CompleteSettledPrefixRead<'_>, CompleteSettledPrefixError> {
        self.require_usable_connection()?;
        if !topology.contains_pg(PgId::new(self.pg_id)) {
            return Err(StoreError::RouteCapabilitySubjectMismatch {
                operation: "complete settled prefix configured PG",
            }
            .into());
        }
        let snapshot = self.command_build_snapshot()?;
        let state =
            self.require_complete_settled_prefix_in_transaction(node_id, epoch, topology)?;
        Ok(CompleteSettledPrefixRead { snapshot, state })
    }

    /// Read-only check in an already acquired transaction; does not establish
    /// validation or authorize mutation. Allocation compares its captured prefix
    /// here after writer admission, before consuming the local cursor.
    pub(in crate::node_runtime::pg_store) fn require_complete_settled_prefix_in_transaction(
        &self,
        node_id: u32,
        epoch: ClusterEpoch,
        topology: &PgTopology,
    ) -> Result<MetadataCommandReplicaState, CompleteSettledPrefixError> {
        self.require_usable_connection()?;
        if self.conn.is_autocommit() || !topology.contains_pg(PgId::new(self.pg_id)) {
            return Err(StoreError::RouteCapabilitySubjectMismatch {
                operation: "complete prefix requires configured transaction",
            }
            .into());
        }
        // Pin the database view BEFORE comparing the external-write stamp. An
        // external writer after this point cannot change this snapshot; the
        // next lookup must observe its changed data_version and reject.
        let state = self.metadata_command_replica_state()?;
        if state.cluster_epoch != epoch {
            return Err(StoreError::StaleMetadataCommand {
                node_id,
                pg_id: self.pg_id,
                command_epoch: state.cluster_epoch,
                current_epoch: epoch,
            }
            .into());
        }
        self.complete_prefix_validation
            .require(self, topology, &state)?;
        // Presence only: do not decode a potentially multi-MiB command just to
        // reject settlement. Even an older-epoch or already-applied slot blocks.
        if self
            .query_row_cached_optional(
                "SELECT 1 FROM metadata_command_pending_slot LIMIT 1",
                [],
                "inspect complete settled prefix pending barrier",
                |_| Ok(()),
            )?
            .is_some()
        {
            return Err(StoreError::MetadataCommandContention {
                context: "inspect complete settled prefix with pending command",
            }
            .into());
        }
        self.require_complete_prefix_evidence_bound(node_id, &state)?;
        Ok(state)
    }
}
