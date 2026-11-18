// Copyright The Argmin Authors.
// SPDX-License-Identifier: Apache-2.0

//! Local complete-prefix startup recovery, not acting-set convergence. Live
//! startup dispatch remains current-only until coordinated activation. Nothing
//! here clears pending custody, applies payloads, or authorizes physical cleanup.

use super::*;
use crate::PgTopology;

#[cfg(test)]
thread_local! {
    static AFTER_RECOVERY_COMMIT: std::cell::RefCell<Option<Box<dyn FnOnce()>>> =
        const { std::cell::RefCell::new(None) };
}

struct RecoveryBoundary<'a> {
    pg: &'a PgStore,
    finished: bool,
}

impl Drop for RecoveryBoundary<'_> {
    fn drop(&mut self) {
        if !self.finished {
            // Cache repair may have marked a transaction-local revision clean.
            // Never retain that volatile observation after rollback or unwind.
            self.pg.invalidate_clean_metadata_digest_revision();
        }
        if !self.pg.conn.is_autocommit() {
            self.pg.retire_connection();
        }
    }
}

impl PgStore {
    // Validate the subjects of ALL retained carriers, including older epochs.
    // Their primary keys are epoch-leading, so proving absence of a foreign PG
    // can scan retained keys. Do this only in full validation before minting the
    // cache; its change stamps revoke the proof after any database mutation.
    fn require_complete_retained_evidence_pg(&self) -> Result<(), StoreError> {
        for (sql, operation) in [
            (
                "SELECT 1 FROM metadata_command_log WHERE pg_id <> ?1 LIMIT 1",
                "complete startup log PG",
            ),
            (
                "SELECT 1 FROM metadata_command_terminal_receipts WHERE pg_id <> ?1 LIMIT 1",
                "complete startup terminal receipt PG",
            ),
            (
                "SELECT 1 FROM metadata_command_checkpoints WHERE pg_id <> ?1 LIMIT 1",
                "complete startup checkpoint PG",
            ),
        ] {
            if self
                .query_row_cached_optional(sql, [self.pg_id], operation, |_| Ok(()))?
                .is_some()
            {
                return Err(StoreError::RouteCapabilitySubjectMismatch { operation });
            }
        }
        Ok(())
    }

    fn require_complete_retained_evidence_pg_with_work(
        &self,
        work: &mut PrefixValidationWork,
    ) -> Result<(), StoreError> {
        for table in [
            "metadata_command_log",
            "metadata_command_terminal_receipts",
            "metadata_command_checkpoints",
        ] {
            work.check()?;
            // Enumerate keys rather than hiding an unbounded foreign-PG scan
            // inside SELECT ... WHERE pg_id <> ?. Charge even matching keys.
            let mut stmt = self
                .conn
                .prepare_cached(&format!("SELECT pg_id FROM {table}"))
                .map_err(|e| StoreError::Db {
                    context: "scan complete validation evidence PGs",
                    source: e.into(),
                })?;
            let mut rows = stmt.query([]).map_err(|e| StoreError::Db {
                context: "scan complete validation evidence PGs",
                source: e.into(),
            })?;
            while let Some(row) = rows.next().map_err(|e| StoreError::Db {
                context: "read complete validation evidence PG",
                source: e.into(),
            })? {
                work.row(4)?;
                if row.get::<_, u32>(0).map_err(|e| StoreError::Db {
                    context: "decode complete validation evidence PG",
                    source: e.into(),
                })? != self.pg_id
                {
                    return Err(StoreError::RouteCapabilitySubjectMismatch {
                        operation: "complete validation retained evidence PG",
                    });
                }
            }
        }
        Ok(())
    }

    /// Constructor-only entry for node startup. Derive the path from the held
    /// exclusive root guard and do not expose this connection before recovery.
    /// The mutating recovery entry below remains private to PG storage.
    pub(in crate::node_runtime) fn open_complete_for_startup(
        guard: &crate::node_runtime::server::StorageNodeDataDirGuard,
        pg_id: u32,
        context: crate::node_runtime::pg_store::PgStoreRecoveryContext,
        topology: &PgTopology,
    ) -> Result<Self, StoreError> {
        if !topology.contains_pg(PgId::new(pg_id)) {
            return Err(StoreError::RouteCapabilitySubjectMismatch {
                operation: "complete startup configured PG",
            });
        }
        let path = guard.locked_data_dir().join(format!("pg-{pg_id:04}"));
        let pg = Self::open(&path, pg_id)?;
        pg.recover_complete_command_state(context, topology)?;
        Ok(pg)
    }

    /// Own one transaction over the stored epoch/prefix, pending indexes and
    /// materialized state. A local terminal record never proves fanout finished.
    /// This returns only replica state, never a command or cleanup capability.
    pub(in crate::node_runtime::pg_store) fn recover_complete_command_state(
        &self,
        context: crate::node_runtime::pg_store::PgStoreRecoveryContext,
        topology: &PgTopology,
    ) -> Result<MetadataCommandReplicaState, StoreError> {
        self.require_usable_connection()?;
        self.complete_prefix_validation.invalidate();
        if !self.conn.is_autocommit() || !topology.contains_pg(PgId::new(self.pg_id)) {
            return Err(StoreError::RouteCapabilitySubjectMismatch {
                operation: "complete startup transaction/topology",
            });
        }
        let node_id = context.node_id().as_u32();
        let mut boundary = RecoveryBoundary {
            pg: self,
            finished: false,
        };
        let (state, validated, revision) =
            self.with_metadata_command_checkpoint_transaction(|| {
                let state = self.metadata_command_replica_state()?;
                let epoch = state.cluster_epoch;
                // Tombstone recording and its prefix link may have been interrupted.
                // This shared tail routine verifies only contiguous V2 tombstones.
                // Its writes remain uncommitted until ALL checks below succeed.
                let state =
                    self.advance_abandoned_metadata_command_log_tail(node_id, epoch, state)?;
                self.validate_complete_prefix_state(node_id, topology, &state, None)?;
                self.repair_metadata_table_digest_cache_drift(node_id)?;
                // Recovering retained state remains useful on a retired allocator,
                // but cannot make that runtime eligible to confirm new effects.
                let validated = if self.require_live_allocation_lifetime().is_ok() {
                    Some(ValidatedPrefix {
                        topology: topology.clone(),
                        pg_id: self.pg_id,
                        state,
                        stamp: Stamp::read(self)?,
                    })
                } else {
                    None
                };
                Ok((state, validated, self.metadata_digest_revision()?))
            })?;
        #[cfg(test)]
        if let Some(hook) = AFTER_RECOVERY_COMMIT.with(|slot| slot.borrow_mut().take()) {
            hook();
        }
        // Only the transaction's validated revision is clean. A later writer
        // must not be blessed by a post-commit revision reload.
        self.mark_metadata_state_digest_clean_at_revision(revision);
        if let Some(validated) = validated {
            self.complete_prefix_validation
                .retain_committed(self, validated)?;
        }
        boundary.finished = true;
        Ok(state)
    }

    // Shared read-only evidence checks. Startup may link tombstones before this
    // call; request reinspection must not repair or advance anything.
    fn validate_complete_prefix_state(
        &self,
        node_id: u32,
        topology: &PgTopology,
        state: &MetadataCommandReplicaState,
        mut work: Option<&mut PrefixValidationWork>,
    ) -> Result<(), StoreError> {
        if let Some(work) = work.as_deref_mut() {
            self.require_complete_retained_evidence_pg_with_work(work)?;
        } else {
            self.require_complete_retained_evidence_pg()?;
        }
        self.require_complete_prefix_evidence_bound(node_id, state)?;
        self.validate_metadata_command_log_suffix_with_work(
            node_id,
            state.cluster_epoch,
            state,
            "validate complete metadata command prefix",
            (true, work.as_deref_mut()),
            TerminalLogGrammar::Complete(topology),
        )?;
        let actual_digest = self.metadata_state_digest_with_work(work.as_deref_mut())?;
        if state.state_digest.value() != actual_digest {
            return Err(StoreError::MetadataStateDigestMismatch {
                node_id,
                pg_id: self.pg_id,
                cluster_epoch: state.cluster_epoch,
                expected_digest: state.state_digest.value(),
                actual_digest,
            });
        }
        if let Some(work) = work.as_deref_mut() {
            work.check()?;
            if let Some(len) = self.query_row_cached_optional(
                "SELECT length(command_bytes) FROM metadata_command_pending_slot",
                [],
                "size complete validation pending context",
                |row| row.get::<_, usize>(0),
            )? {
                work.row(len)?;
            }
        }
        if let Some(pending) =
            self.inspect_complete_pending_in_open_transaction(node_id, topology)?
        {
            if pending.id.cluster_epoch() > state.cluster_epoch {
                return Err(StoreError::MetadataCommandLogConflict {
                    node_id,
                    pg_id: self.pg_id,
                    cluster_epoch: pending.id.cluster_epoch(),
                    log_index: pending.id.log_index().get(),
                });
            }
        }
        if let Some(work) = work {
            work.check()?;
        }
        Ok(())
    }

    /// Explicit read-only validation for admitted Direct PUT reinspection. This
    /// never links a tail, repairs digest tables, clears a slot or commits SQL.
    /// The caller must check its retained admission/deadline before and after;
    /// this local cache entry confers no mutation or physical-write authority.
    pub(in crate::node_runtime::pg_store) fn validate_complete_direct_reinspection_prefix(
        &self,
        node: NodeId,
        epoch: ClusterEpoch,
        topology: &PgTopology,
        deadline: Instant,
        fence: AdmittedRouteEffectFence,
    ) -> Result<(), CompleteSettledPrefixError> {
        let node_id = node.as_u32();
        fence.require_valid_for(epoch)?;
        let mut work = PrefixValidationWork::new(deadline, fence);
        work.check()?;
        if !topology.contains_pg(PgId::new(self.pg_id)) {
            return Err(StoreError::RouteCapabilitySubjectMismatch {
                operation: "complete reinspection configured PG",
            }
            .into());
        }
        let snapshot = self.command_build_snapshot()?;
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
        match self
            .complete_prefix_validation
            .require(self, topology, &state)
        {
            Ok(()) => {
                snapshot.finish()?;
                work.check()?;
                return Ok(());
            }
            Err(CompleteSettledPrefixError::ValidationRequired) => (),
            Err(error) => return Err(error),
        }
        self.complete_prefix_validation.invalidate();
        self.validate_complete_prefix_state(node_id, topology, &state, Some(&mut work))?;
        // Unlike startup, this read-only path cannot repair digest-cache drift.
        // The heartbeat check compares its aggregate with the validated state;
        // query_only also prevents its missing-row initializer from writing.
        self.metadata_command_replica_state_for_heartbeat(node_id, epoch)?;
        work.check()?;
        let validated = ValidatedPrefix {
            topology: topology.clone(),
            pg_id: self.pg_id,
            state,
            stamp: Stamp::read(self)?,
        };
        snapshot.finish()?;
        work.check()?;
        // Do not bless a writer racing read-snapshot teardown. A mismatch leaves
        // the cache cold, so the ensuing bounded snapshot fails closed.
        self.complete_prefix_validation
            .retain_committed(self, validated)?;
        Ok(())
    }

    #[cfg(test)]
    pub(in crate::node_runtime::pg_store) fn test_after_complete_recovery_commit(
        &self,
        hook: impl FnOnce() + 'static,
    ) {
        AFTER_RECOVERY_COMMIT.with(|slot| {
            assert!(slot.borrow_mut().replace(Box::new(hook)).is_none());
        });
    }
}
