// Copyright The Argmin Authors.
// SPDX-License-Identifier: Apache-2.0

use std::collections::BTreeSet;
use std::future::Future;
use std::path::Path;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use placement::NodeId;
use tokio::runtime::Handle;

use crate::control_plane::{
    load_authority_clock_restart_checkpoint_for_startup, ClusterControlSnapshot,
    ClusterRuntimeMapSnapshot, ControlPlaneAdmin, ControlPlaneAuthorityClock,
    ControlPlaneAuthorityClockCheckpointBinding, ControlPlaneAuthorityClockContext,
    ControlPlaneAuthorityClockRestartCheckpoint, ControlPlaneError, ControlPlaneHeartbeatRefresh,
    ControlPlaneHeartbeatRuntimeMapSource, ControlPlaneRpcResponsePublication,
    ControlPlaneRuntimeMapDiagnosticSnapshot, ControlPlaneRuntimeMapSource,
    ControlPlaneRuntimeMapStatus, FencedPgMetadataTransferSnapshot, LeaseHorizonAuthorityBinding,
    NodeHeartbeat, PgMetadataTransferProof, UnavailablePgReconciliationCandidate,
    UnavailablePgReconciliationCompletionBatch, UnavailablePgReconciliationCursor,
    UnavailablePgReconciliationPollBatch, UnavailablePgReconciliationStage,
    UnavailablePgReconciliationWork,
};
use crate::control_plane_command::{ControlPlaneCommand, ControlPlaneCommandResponse};
use crate::control_plane_raft::{
    ControlPlaneRaftAuthority, ControlPlaneRaftAuthorityStatus, ControlPlaneRaftCommandOutcome,
    ControlPlaneRaftNodeId, SubmittedControlPlaneRaftCommand,
};
use crate::control_plane_raft_durability::ControlPlaneRaftAuthorityDurability;
use crate::control_plane_server_bootstrap::{
    ControlPlaneRpcServerBootstrap, ControlPlaneRpcServerLoops,
};
use crate::static_topology::UncertifiedInitialControlPlaneTopology;
use crate::types::{ClusterEpoch, PgId, PgState};

#[cfg(test)]
type UnavailableReconciliationCommandDerivedHook = Arc<Mutex<Option<Arc<dyn Fn() + Send + Sync>>>>;
#[cfg(test)]
type UnavailableReconciliationTimeSampledHook = Arc<Mutex<Option<Arc<dyn Fn(u64) + Send + Sync>>>>;

/// Storage-owned logical host for one durable Raft control-plane authority.
///
/// The host derives its durability lifecycle and authority clock from the exact
/// retained authority. Callers cannot pair an authority with an independent
/// checkpoint domain, clock checkpoint, or response-publication capability.
#[derive(Clone)]
pub struct ControlPlaneRaftAuthorityHost {
    runtime: Handle,
    authority: Arc<ControlPlaneRaftAuthority>,
    durability: Option<ControlPlaneRaftAuthorityDurability>,
    resample_authority_time: bool,
    authority_clock: Option<Arc<Mutex<ControlPlaneAuthorityClock>>>,
    #[cfg(any(test, feature = "test-hooks"))]
    after_heartbeat_commit_term: Arc<Mutex<Option<u64>>>,
    #[cfg(test)]
    after_unavailable_reconciliation_command_derived: UnavailableReconciliationCommandDerivedHook,
    #[cfg(test)]
    after_unavailable_reconciliation_time_sampled: UnavailableReconciliationTimeSampledHook,
}

pub(crate) struct DurableAuthorityHostLifecycle {
    runtime: Handle,
    durability: ControlPlaneRaftAuthorityDurability,
    authority_clock: Arc<Mutex<ControlPlaneAuthorityClock>>,
}

/// Authority-clock restart evidence classified before a durable Raft
/// authority is opened or any of its listeners are published.
///
/// The binding is retained so this capability cannot be consumed by a
/// different cluster/node authority after preflight.
pub(crate) struct PreparedControlPlaneRaftAuthorityClock {
    binding: ControlPlaneAuthorityClockCheckpointBinding,
    restart_checkpoint: Option<ControlPlaneAuthorityClockRestartCheckpoint>,
}

/// A host whose checkpoint and clock state have been validated, but whose
/// fresh-cluster leadership-term decision may still depend on startup
/// membership convergence.
pub(crate) struct PreparedControlPlaneRaftAuthorityHost {
    host: ControlPlaneRaftAuthorityHost,
    initial_term_binding_pending: bool,
}

impl PreparedControlPlaneRaftAuthorityHost {
    pub(crate) fn finish_startup(self) -> Result<ControlPlaneRaftAuthorityHost, ControlPlaneError> {
        if self.initial_term_binding_pending {
            let status =
                block_on_control_plane_raft(&self.host.runtime, self.host.authority.status())?;
            let mut authority_clock = self
                .host
                .authority_clock
                .as_ref()
                .expect("prepared durable host must retain its authority clock")
                .lock()
                .expect("control-plane authority clock mutex poisoned");
            if status.local_leader() {
                let current_term = status.current_term().ok_or_else(|| {
                    ControlPlaneError::invariant_failure(
                        "local OpenRaft leader has no current term at startup",
                    )
                })?;
                authority_clock.bind_initial_raft_leadership_term(Some(current_term));
            } else {
                // A fresh joining follower must not retain the first-term
                // allowance after startup convergence.
                authority_clock.bind_initial_raft_leadership_term(None);
            }
        }
        Ok(self.host)
    }
}

impl PreparedControlPlaneRaftAuthorityClock {
    pub(crate) fn load(
        artifact_path: &Path,
        cluster_name: &str,
        node_id: ControlPlaneRaftNodeId,
    ) -> Result<Self, ControlPlaneError> {
        let binding = ControlPlaneAuthorityClockCheckpointBinding::for_raft(cluster_name, node_id);
        Self::load_for_binding(artifact_path, binding)
    }

    fn load_for_binding(
        artifact_path: &Path,
        binding: ControlPlaneAuthorityClockCheckpointBinding,
    ) -> Result<Self, ControlPlaneError> {
        let restart_checkpoint =
            load_authority_clock_restart_checkpoint_for_startup(artifact_path, binding)?;
        Ok(Self {
            binding,
            restart_checkpoint,
        })
    }

    fn into_restart_checkpoint(
        self,
        authority: &ControlPlaneRaftAuthority,
    ) -> Result<Option<ControlPlaneAuthorityClockRestartCheckpoint>, ControlPlaneError> {
        if self.binding != authority.authority_clock_checkpoint_binding() {
            return Err(ControlPlaneError::invariant_failure(
                "prepared authority-clock restart evidence belongs to another Raft authority",
            ));
        }
        Ok(self.restart_checkpoint)
    }
}

impl std::fmt::Debug for ControlPlaneRaftAuthorityHost {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ControlPlaneRaftAuthorityHost")
            .field("durable", &self.durability.is_some())
            .field("authority_clock", &self.authority_clock.is_some())
            .finish_non_exhaustive()
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ControlPlaneRaftHeartbeatLeaseExpiry {
    cluster_epoch: ClusterEpoch,
    expired_nodes: usize,
    peering_pgs: usize,
}

impl ControlPlaneRaftHeartbeatLeaseExpiry {
    #[must_use]
    pub fn cluster_epoch(self) -> ClusterEpoch {
        self.cluster_epoch
    }

    #[must_use]
    pub fn expired_nodes(self) -> usize {
        self.expired_nodes
    }

    #[must_use]
    pub fn peering_pgs(self) -> usize {
        self.peering_pgs
    }
}

impl ControlPlaneRaftAuthorityHost {
    pub(crate) fn start_prepared(
        runtime: Handle,
        authority: Arc<ControlPlaneRaftAuthority>,
        prepared_clock: PreparedControlPlaneRaftAuthorityClock,
    ) -> Result<PreparedControlPlaneRaftAuthorityHost, ControlPlaneError> {
        let restart_checkpoint = prepared_clock.into_restart_checkpoint(&authority)?;
        Self::start_with_restart_checkpoint(runtime, authority, restart_checkpoint)
    }

    #[cfg(test)]
    pub(crate) fn start_durable(
        runtime: Handle,
        authority: Arc<ControlPlaneRaftAuthority>,
    ) -> Result<Self, ControlPlaneError> {
        let artifact_path = authority
            .configured_durable_artifact_path()
            .map_err(|error| {
                error.into_durability_failure(
                    "control-plane Raft authority host requires configured durable state",
                )
            })?;
        let prepared_clock = PreparedControlPlaneRaftAuthorityClock::load_for_binding(
            &artifact_path,
            authority.authority_clock_checkpoint_binding(),
        )?;
        Self::start_prepared(runtime, authority, prepared_clock)?.finish_startup()
    }

    fn start_with_restart_checkpoint(
        runtime: Handle,
        authority: Arc<ControlPlaneRaftAuthority>,
        restart_checkpoint: Option<ControlPlaneAuthorityClockRestartCheckpoint>,
    ) -> Result<PreparedControlPlaneRaftAuthorityHost, ControlPlaneError> {
        if let Some(lifecycle) = authority
            .authority_host_lifecycle_slot()
            .get()
            .map(Arc::clone)
        {
            return Ok(PreparedControlPlaneRaftAuthorityHost {
                host: Self::from_durable_lifecycle(authority, &lifecycle),
                initial_term_binding_pending: false,
            });
        }
        let durability = authority.durability_lifecycle(runtime)?;
        let runtime = durability.runtime();
        let initial_snapshot =
            block_on_control_plane_raft(&runtime, authority.current_control_plane_snapshot())?;
        let initial_status = block_on_control_plane_raft(&runtime, authority.status())?;
        let mut authority_clock =
            ControlPlaneAuthorityClock::new_from_process_clock_with_restart_checkpoint(
                initial_snapshot.max_committed_timestamp_ms(),
                restart_checkpoint,
            )?;
        if !authority_clock.is_established() {
            durability.invalidate_authority_clock_restart_checkpoint()?;
        }
        if let Some(previous_authority) = initial_snapshot.lease_grant_horizon_authority() {
            authority_clock.advance_generation_past_lease_horizon(previous_authority)?;
        }
        let initial_term_binding_pending = prepare_initial_raft_leadership_term(
            &mut authority_clock,
            initial_status.local_leader(),
            initial_status.current_term(),
        )?;
        let lifecycle = Arc::new(DurableAuthorityHostLifecycle {
            runtime,
            durability,
            authority_clock: Arc::new(Mutex::new(authority_clock)),
        });
        let lifecycle = Arc::clone(
            authority
                .authority_host_lifecycle_slot()
                .get_or_init(|| lifecycle),
        );
        Ok(PreparedControlPlaneRaftAuthorityHost {
            host: Self::from_durable_lifecycle(authority, &lifecycle),
            initial_term_binding_pending,
        })
    }

    fn from_durable_lifecycle(
        authority: Arc<ControlPlaneRaftAuthority>,
        lifecycle: &Arc<DurableAuthorityHostLifecycle>,
    ) -> Self {
        Self {
            runtime: lifecycle.runtime.clone(),
            authority,
            durability: Some(lifecycle.durability.clone()),
            resample_authority_time: true,
            authority_clock: Some(Arc::clone(&lifecycle.authority_clock)),
            #[cfg(any(test, feature = "test-hooks"))]
            after_heartbeat_commit_term: Arc::new(Mutex::new(None)),
            #[cfg(test)]
            after_unavailable_reconciliation_command_derived: Arc::new(Mutex::new(None)),
            #[cfg(test)]
            after_unavailable_reconciliation_time_sampled: Arc::new(Mutex::new(None)),
        }
    }

    pub fn serve_rpc(
        &self,
        server: ControlPlaneRpcServerBootstrap,
        terminal_failure_handler: Arc<dyn Fn() + Send + Sync>,
    ) -> Result<ControlPlaneRpcServerLoops, ControlPlaneError> {
        let authority_clock = Arc::clone(self.authority_clock.as_ref().ok_or_else(|| {
            ControlPlaneError::invariant_failure(
                "durable Raft authority host has no authority clock",
            )
        })?);
        let durability = self.durability.as_ref().ok_or_else(|| {
            ControlPlaneError::invariant_failure(
                "durable Raft authority host has no durability lifecycle",
            )
        })?;
        let checkpoint_target = durability.authority_clock_checkpoint_target();
        let runtime = self.runtime.clone();
        let authority = Arc::clone(&self.authority);
        let authority_confirmation: Arc<dyn Fn() -> Result<(), ControlPlaneError> + Send + Sync> =
            Arc::new(move || {
                block_on_control_plane_raft(
                    &runtime,
                    authority.confirmed_linearized_authority_status(),
                )
                .map(|_| ())
            });
        let response_publication: Arc<dyn ControlPlaneRpcResponsePublication> =
            Arc::new(self.authority.durability_publication()?);
        Ok(server.serve_cloned_raft_authority(
            self.clone(),
            authority_clock,
            checkpoint_target,
            authority_confirmation,
            response_publication,
            terminal_failure_handler,
        ))
    }

    pub fn establish_uncertified_initial_topology(
        &self,
        topology: &UncertifiedInitialControlPlaneTopology,
    ) -> Result<Option<u64>, ControlPlaneError> {
        self.block_on(
            self.authority
                .establish_uncertified_initial_control_plane_topology(topology),
        )
    }

    pub fn linearized_authority_serving(&self) -> Result<bool, ControlPlaneError> {
        Ok(self
            .block_on(self.authority.status())?
            .linearized_authority_serving())
    }

    pub fn invalidate_blocked_authority_clock_checkpoint(&self) -> Result<(), ControlPlaneError> {
        let authority_clock = self
            .authority_clock
            .as_ref()
            .ok_or_else(|| {
                ControlPlaneError::invariant_failure(
                    "durable Raft authority host has no authority clock",
                )
            })?
            .lock()
            .expect("control-plane authority clock mutex poisoned");
        self.durability
            .as_ref()
            .ok_or_else(|| {
                ControlPlaneError::invariant_failure(
                    "durable Raft authority host has no durability lifecycle",
                )
            })?
            .authority_clock_checkpoint_target()
            .invalidate_if_blocked(&authority_clock)
    }

    pub fn expire_heartbeat_leases(
        &mut self,
        now_ms: u64,
    ) -> Result<ControlPlaneRaftHeartbeatLeaseExpiry, ControlPlaneError> {
        let (preflight_now_ms, preflight_authority) =
            self.local_authority_time_and_lease_horizon_binding(now_ms)?;
        let preflight_snapshot = self.current_snapshot()?;
        let preflight_expired = preflight_snapshot.expired_node_heartbeat_leases(
            preflight_snapshot.heartbeat_lease_expiry_timestamp(preflight_now_ms),
        );
        if preflight_expired.is_empty() {
            return Ok(ControlPlaneRaftHeartbeatLeaseExpiry {
                cluster_epoch: preflight_snapshot.cluster_epoch(),
                expired_nodes: 0,
                peering_pgs: 0,
            });
        }
        let preflight_authority = preflight_authority.ok_or_else(|| {
            ControlPlaneError::invariant_failure(
                "OpenRaft heartbeat expiry has no serving lease-horizon authority",
            )
        })?;
        preflight_snapshot
            .validate_lease_grant_horizon_rebinding(preflight_authority, preflight_now_ms)?;

        let (now_ms, lease_horizon_authority) =
            self.authority_time_and_lease_horizon_binding(now_ms)?;
        let snapshot = self.current_snapshot()?;
        let expire_at_ms = snapshot.heartbeat_lease_expiry_timestamp(now_ms);
        let expired = snapshot.expired_node_heartbeat_leases(expire_at_ms);
        if expired.is_empty() {
            return Ok(ControlPlaneRaftHeartbeatLeaseExpiry {
                cluster_epoch: snapshot.cluster_epoch(),
                expired_nodes: 0,
                peering_pgs: 0,
            });
        }
        let authority = lease_horizon_authority.ok_or_else(|| {
            ControlPlaneError::invariant_failure(
                "OpenRaft heartbeat expiry has no serving lease-horizon authority",
            )
        })?;
        snapshot.validate_lease_grant_horizon_rebinding(authority, now_ms)?;
        let response =
            self.submit_raft_liveness_command(ControlPlaneCommand::ExpireNodeHeartbeatLeases {
                authority,
                expire_at_ms,
                expired,
            })?;
        let ControlPlaneCommandResponse::ExpireHeartbeatLeases {
            expired_nodes,
            peering_pgs,
        } = response
        else {
            unreachable!("heartbeat lease expiry command returned the wrong response");
        };
        Ok(ControlPlaneRaftHeartbeatLeaseExpiry {
            cluster_epoch: self.current_snapshot()?.cluster_epoch(),
            expired_nodes: expired_nodes.len(),
            peering_pgs: peering_pgs.len(),
        })
    }

    pub fn poll_unavailable_pg_reconciliation(
        &mut self,
        cursor: &mut UnavailablePgReconciliationCursor,
        supplied_now_ms: u64,
    ) -> Result<Option<UnavailablePgReconciliationWork>, ControlPlaneError> {
        let now_ms = self.authority_now_ms(supplied_now_ms)?;
        let snapshot = self.current_snapshot()?;
        let scan = snapshot.scan_unavailable_pg_reconciliation(*cursor, now_ms);
        *cursor = scan.next_cursor;
        match scan.candidate {
            None => Ok(None),
            Some(UnavailablePgReconciliationCandidate::Resume(work)) => Ok(Some(work)),
            Some(UnavailablePgReconciliationCandidate::Begin {
                pg_id,
                unavailable_node_id,
            }) => {
                #[cfg(test)]
                self.run_after_unavailable_reconciliation_time_sampled_hook(now_ms);
                #[cfg(test)]
                let after_derived = self.take_unavailable_reconciliation_command_derived_hook();
                self.submit_raft_command_derived(|current| {
                    let command_now_ms = authority_time_not_before_snapshot(current, now_ms);
                    let command = current.begin_unavailable_pg_placement_transition_batch_command(
                        &[(pg_id, unavailable_node_id)],
                        command_now_ms,
                    )?;
                    #[cfg(test)]
                    if let Some(after_derived) = after_derived {
                        after_derived();
                    }
                    Ok(command)
                })?;
                let current = self.current_snapshot()?;
                let transition = current
                    .unavailable_pg_placement_transition(pg_id)
                    .ok_or_else(|| {
                        ControlPlaneError::invariant_failure(
                            "committed unavailable PG transition is absent from current state",
                        )
                    })?;
                Ok(Some(UnavailablePgReconciliationWork::from_transition(
                    transition,
                    UnavailablePgReconciliationStage::MetadataTransfer,
                )))
            }
        }
    }

    pub(crate) fn poll_unavailable_pg_reconciliation_batch(
        &mut self,
        cursor: &mut UnavailablePgReconciliationCursor,
        supplied_now_ms: u64,
    ) -> Result<UnavailablePgReconciliationPollBatch, ControlPlaneError> {
        let now_ms = self.authority_now_ms(supplied_now_ms)?;
        let snapshot = self.current_snapshot()?;
        let scan = snapshot.scan_unavailable_pg_reconciliation_batch(*cursor, now_ms);
        *cursor = scan.next_cursor;
        let begin_candidates = scan
            .candidates
            .iter()
            .filter_map(|candidate| match candidate {
                UnavailablePgReconciliationCandidate::Begin {
                    pg_id,
                    unavailable_node_id,
                } => Some((*pg_id, *unavailable_node_id)),
                UnavailablePgReconciliationCandidate::Resume(_) => None,
            })
            .collect::<Vec<_>>();
        let mut begun = Vec::new();
        let mut rejected = Vec::new();
        if !begin_candidates.is_empty() {
            #[cfg(test)]
            self.run_after_unavailable_reconciliation_time_sampled_hook(now_ms);
        }
        #[cfg(test)]
        let after_derived = self.take_unavailable_reconciliation_command_derived_hook();
        if !begin_candidates.is_empty() {
            let mut prepared_slot = None;
            let submit_result = self.submit_raft_command_derived(|current| {
                let command_now_ms = authority_time_not_before_snapshot(current, now_ms);
                let prepared = current.prepare_unavailable_pg_placement_transition_batch(
                    &begin_candidates,
                    command_now_ms,
                )?;
                let command = prepared.command.clone();
                prepared_slot = Some(prepared);
                #[cfg(test)]
                if command.is_some() {
                    if let Some(after_derived) = after_derived {
                        after_derived();
                    }
                }
                command.ok_or_else(|| ControlPlaneError::CommandDecode {
                    message: "unavailable transition begin page has no valid member".to_owned(),
                })
            });
            let (prepared, submit_result) = resolve_derived_preparation(
                prepared_slot,
                submit_result,
                "unavailable transition begin derivation returned without preparation state",
            )?;
            let included_pg_ids = prepared
                .included
                .iter()
                .map(|(pg_id, _)| *pg_id)
                .collect::<BTreeSet<_>>();
            let submitted = prepared.command.is_some();
            if submitted {
                submit_result?;
                begun.extend(included_pg_ids.iter().copied());
            }
            rejected.extend(prepared.rejected);
        }
        let current = self.current_snapshot()?;
        let mut work = scan
            .candidates
            .into_iter()
            .filter_map(|candidate| match candidate {
                UnavailablePgReconciliationCandidate::Resume(work) => Some(work),
                UnavailablePgReconciliationCandidate::Begin { .. } => None,
            })
            .collect::<Vec<_>>();
        for pg_id in begun {
            let transition = current
                .unavailable_pg_placement_transition(pg_id)
                .ok_or_else(|| {
                    ControlPlaneError::invariant_failure(
                        "committed unavailable PG transition is absent from current state",
                    )
                })?;
            work.push(UnavailablePgReconciliationWork::from_transition(
                transition,
                UnavailablePgReconciliationStage::MetadataTransfer,
            ));
        }
        work.sort_by_key(UnavailablePgReconciliationWork::pg_id);
        Ok(UnavailablePgReconciliationPollBatch { work, rejected })
    }

    pub fn complete_unavailable_pg_reconciliation(
        &mut self,
        work: &UnavailablePgReconciliationWork,
        supplied_now_ms: u64,
    ) -> Result<bool, ControlPlaneError> {
        let now_ms = self.authority_now_ms(supplied_now_ms)?;
        let snapshot = self.current_snapshot()?;
        let Some(transition) = snapshot.unavailable_pg_placement_transition(work.pg_id()) else {
            return Ok(false);
        };
        if !work.mutation_binding().matches_transition(transition) {
            return Ok(false);
        }
        #[cfg(test)]
        self.run_after_unavailable_reconciliation_time_sampled_hook(now_ms);
        #[cfg(test)]
        let after_derived = self.take_unavailable_reconciliation_command_derived_hook();
        self.submit_raft_command_derived(|current| {
            let command_now_ms = authority_time_not_before_snapshot(current, now_ms);
            let command = current.complete_unavailable_pg_placement_transition_batch_command(
                std::slice::from_ref(work),
                command_now_ms,
            )?;
            #[cfg(test)]
            if let Some(after_derived) = after_derived {
                after_derived();
            }
            Ok(command)
        })?;
        Ok(true)
    }

    pub(crate) fn complete_unavailable_pg_reconciliation_batch(
        &mut self,
        work: &[UnavailablePgReconciliationWork],
        supplied_now_ms: u64,
    ) -> Result<UnavailablePgReconciliationCompletionBatch, ControlPlaneError> {
        let now_ms = self.authority_now_ms(supplied_now_ms)?;
        #[cfg(test)]
        self.run_after_unavailable_reconciliation_time_sampled_hook(now_ms);
        #[cfg(test)]
        let after_derived = self.take_unavailable_reconciliation_command_derived_hook();
        let mut prepared_slot = None;
        let submit_result = self.submit_raft_command_derived(|current| {
            let command_now_ms = authority_time_not_before_snapshot(current, now_ms);
            let prepared =
                current.prepare_unavailable_pg_placement_completion_batch(work, command_now_ms)?;
            let command = prepared.command.clone();
            prepared_slot = Some(prepared);
            #[cfg(test)]
            if command.is_some() {
                if let Some(after_derived) = after_derived {
                    after_derived();
                }
            }
            command.ok_or_else(|| ControlPlaneError::CommandDecode {
                message: "unavailable transition completion batch has no valid member".to_owned(),
            })
        });
        let (prepared, submit_result) = resolve_derived_preparation(
            prepared_slot,
            submit_result,
            "unavailable transition completion derivation returned without preparation state",
        )?;
        if prepared.command.is_some() {
            submit_result?;
        }
        let included_pg_ids = prepared
            .included
            .iter()
            .map(UnavailablePgReconciliationWork::pg_id)
            .collect::<BTreeSet<_>>();
        let rejected_pg_ids = prepared
            .rejected
            .iter()
            .map(|(work, _)| work.pg_id())
            .collect::<BTreeSet<_>>();
        let rederive = work
            .iter()
            .filter(|work| {
                !included_pg_ids.contains(&work.pg_id()) && !rejected_pg_ids.contains(&work.pg_id())
            })
            .cloned()
            .collect();
        Ok(UnavailablePgReconciliationCompletionBatch {
            completed: prepared.included,
            rejected: prepared.rejected,
            rederive,
        })
    }

    fn block_on<F: Future>(&self, future: F) -> F::Output {
        block_on_control_plane_raft(&self.runtime, future)
    }

    fn authority_now_ms(&self, supplied_now_ms: u64) -> Result<u64, ControlPlaneError> {
        Ok(self
            .authority_time_and_lease_horizon_binding(supplied_now_ms)?
            .0)
    }

    fn authority_time_and_lease_horizon_binding(
        &self,
        supplied_now_ms: u64,
    ) -> Result<(u64, Option<LeaseHorizonAuthorityBinding>), ControlPlaneError> {
        if !self.resample_authority_time {
            #[cfg(any(test, feature = "test-hooks"))]
            {
                let status = self.block_on(self.authority.status())?;
                let term = status
                    .local_leader()
                    .then(|| status.current_term())
                    .flatten()
                    .ok_or(ControlPlaneError::AuthorityNotServing)?;
                return Ok((
                    supplied_now_ms,
                    LeaseHorizonAuthorityBinding::checked_new(1, Some(term)),
                ));
            }
            #[cfg(not(any(test, feature = "test-hooks")))]
            return Ok((supplied_now_ms, None));
        }
        let status = self.block_on(self.authority.confirmed_linearized_authority_status())?;
        self.authority_time_and_lease_horizon_binding_for_status(status)
    }

    fn local_authority_time_and_lease_horizon_binding(
        &self,
        supplied_now_ms: u64,
    ) -> Result<(u64, Option<LeaseHorizonAuthorityBinding>), ControlPlaneError> {
        if !self.resample_authority_time {
            return self.authority_time_and_lease_horizon_binding(supplied_now_ms);
        }
        let status = self.block_on(self.authority.status())?;
        if !status.linearized_authority_serving() {
            return Err(ControlPlaneError::AuthorityNotServing);
        }
        self.authority_time_and_lease_horizon_binding_for_status(status)
    }

    fn authority_time_and_lease_horizon_binding_for_status(
        &self,
        status: ControlPlaneRaftAuthorityStatus,
    ) -> Result<(u64, Option<LeaseHorizonAuthorityBinding>), ControlPlaneError> {
        let current_term = status.current_term().ok_or_else(|| {
            ControlPlaneError::invariant_failure("local OpenRaft leader has no current term")
        })?;
        self.authority_time_and_lease_horizon_binding_for_term(current_term)
    }

    fn authority_time_and_lease_horizon_binding_for_term(
        &self,
        current_term: u64,
    ) -> Result<(u64, Option<LeaseHorizonAuthorityBinding>), ControlPlaneError> {
        let max_committed_timestamp_ms = self.current_snapshot()?.max_committed_timestamp_ms();
        let mut authority_clock = self
            .authority_clock
            .as_ref()
            .expect("resampled authority time requires a clock gate")
            .lock()
            .expect("control-plane authority clock mutex poisoned");
        authority_clock.observe_committed_timestamp_high_water(max_committed_timestamp_ms);
        authority_clock.validate_raft_leadership_term(current_term)?;
        let authority_now_ms = authority_clock.effective_process_now_ms()?;
        let authority = authority_clock.lease_horizon_authority_binding(Some(current_term))?;
        Ok((authority_now_ms, Some(authority)))
    }

    fn ensure_not_durably_poisoned(&self) -> Result<(), ControlPlaneError> {
        self.authority.durability_publication()?.ensure_available()
    }

    fn poison_durable_authority(&self, message: String) {
        self.authority
            .durability_publication()
            .expect("Raft durability publication should already be initialized")
            .poison(message);
    }

    fn store_durable_restart_artifact(&self) -> Result<(), ControlPlaneError> {
        let Some(durability) = &self.durability else {
            return Ok(());
        };
        durability.store_restart_artifact()
    }

    fn checkpoint_successful_linearized_read(&self) -> Result<(), ControlPlaneError> {
        let Some(durability) = &self.durability else {
            return self.ensure_not_durably_poisoned();
        };
        durability.checkpoint_successful_linearized_read()
    }

    fn submit_raft_command(
        &mut self,
        command: ControlPlaneCommand,
    ) -> Result<ControlPlaneCommandResponse, ControlPlaneError> {
        self.submit_raft_command_with_checkpoint_policy(command, true)
    }

    fn submit_raft_command_derived<F>(
        &mut self,
        derive_command: F,
    ) -> Result<ControlPlaneCommandResponse, ControlPlaneError>
    where
        F: FnOnce(&ClusterControlSnapshot) -> Result<ControlPlaneCommand, ControlPlaneError>,
    {
        self.ensure_not_durably_poisoned()?;
        let submitted = self.block_on(
            self.authority
                .submit_control_plane_command_derived(derive_command),
        )?;
        self.finish_submitted_raft_command(submitted, true)
    }

    fn submit_raft_liveness_command(
        &mut self,
        command: ControlPlaneCommand,
    ) -> Result<ControlPlaneCommandResponse, ControlPlaneError> {
        let wal_backed = self.authority.durability_metric_snapshots().wal.is_some();
        self.submit_raft_command_with_checkpoint_policy(command, !wal_backed)
    }

    fn submit_raft_command_with_checkpoint_policy(
        &mut self,
        command: ControlPlaneCommand,
        checkpoint_after_commit: bool,
    ) -> Result<ControlPlaneCommandResponse, ControlPlaneError> {
        self.ensure_not_durably_poisoned()?;
        let submitted = self.block_on(self.authority.submit_control_plane_command(command))?;
        self.finish_submitted_raft_command(submitted, checkpoint_after_commit)
    }

    fn finish_submitted_raft_command(
        &mut self,
        submitted: SubmittedControlPlaneRaftCommand,
        checkpoint_after_commit: bool,
    ) -> Result<ControlPlaneCommandResponse, ControlPlaneError> {
        let outcome = submitted.into_outcome();
        if checkpoint_after_commit {
            self.checkpoint_committed_raft_command()?;
        }
        match outcome {
            ControlPlaneRaftCommandOutcome::Applied(response) => Ok(response),
            ControlPlaneRaftCommandOutcome::Rejected(error) => Err(error),
        }
    }

    fn checkpoint_committed_raft_command(&self) -> Result<(), ControlPlaneError> {
        if let Err(error) = self.store_durable_restart_artifact() {
            self.poison_durable_authority(format!(
                "OpenRaft control-plane durability checkpoint failed after a committed command; \
                 refusing to serve until restart: {error}"
            ));
            return Err(error);
        }
        Ok(())
    }

    fn current_snapshot(&self) -> Result<ClusterControlSnapshot, ControlPlaneError> {
        self.ensure_not_durably_poisoned()?;
        self.block_on(self.authority.current_control_plane_snapshot())
    }

    #[cfg(any(test, feature = "test-hooks"))]
    fn run_after_heartbeat_commit_hook(&self) -> Option<u64> {
        self.after_heartbeat_commit_term
            .lock()
            .expect("OpenRaft heartbeat test hook mutex poisoned")
            .take()
    }
}

fn prepare_initial_raft_leadership_term(
    authority_clock: &mut ControlPlaneAuthorityClock,
    local_leader: bool,
    current_term: Option<u64>,
) -> Result<bool, ControlPlaneError> {
    if !local_leader {
        // The authority may be fresh, restored, or joining. Startup owns the
        // first point at which its converged local role is known, so defer
        // binding or closing the one-shot allowance until then.
        return Ok(true);
    }
    let current_term = current_term.ok_or_else(|| {
        ControlPlaneError::invariant_failure("local OpenRaft leader has no current term at startup")
    })?;
    authority_clock.bind_initial_raft_leadership_term(Some(current_term));
    Ok(false)
}

#[cfg(any(test, feature = "test-hooks"))]
impl ControlPlaneRaftAuthorityHost {
    pub fn new_for_test(
        runtime: Handle,
        authority: Arc<ControlPlaneRaftAuthority>,
        durable: bool,
    ) -> Result<Self, ControlPlaneError> {
        let durability = durable
            .then(|| authority.durability_lifecycle(runtime.clone()))
            .transpose()?;
        Ok(Self {
            runtime,
            authority,
            durability,
            resample_authority_time: false,
            authority_clock: None,
            after_heartbeat_commit_term: Arc::new(Mutex::new(None)),
            #[cfg(test)]
            after_unavailable_reconciliation_command_derived: Arc::new(Mutex::new(None)),
            #[cfg(test)]
            after_unavailable_reconciliation_time_sampled: Arc::new(Mutex::new(None)),
        })
    }

    pub fn enable_resampled_authority_time_for_test(
        &mut self,
        now_ms: u64,
    ) -> Result<(), ControlPlaneError> {
        let max_committed_timestamp_ms = self.current_snapshot()?.max_committed_timestamp_ms();
        let status = self.block_on(self.authority.status())?;
        let mut authority_clock =
            ControlPlaneAuthorityClock::new(max_committed_timestamp_ms, now_ms, Some(now_ms))?;
        authority_clock.bind_initial_raft_leadership_term(
            status
                .local_leader()
                .then(|| status.current_term())
                .flatten(),
        );
        self.authority_clock = Some(Arc::new(Mutex::new(authority_clock)));
        self.resample_authority_time = true;
        Ok(())
    }

    pub fn block_on_for_test<F: Future>(&self, future: F) -> F::Output {
        self.block_on(future)
    }

    pub fn authority_for_test(&self) -> &Arc<ControlPlaneRaftAuthority> {
        &self.authority
    }

    pub fn durability_for_test(&self) -> Option<&ControlPlaneRaftAuthorityDurability> {
        self.durability.as_ref()
    }

    pub fn current_snapshot_for_test(&self) -> Result<ClusterControlSnapshot, ControlPlaneError> {
        self.current_snapshot()
    }

    pub fn store_restart_artifact_for_test(&self) -> Result<(), ControlPlaneError> {
        self.store_durable_restart_artifact()
    }

    pub fn submit_command_for_test(
        &mut self,
        command: ControlPlaneCommand,
    ) -> Result<ControlPlaneCommandResponse, ControlPlaneError> {
        self.submit_raft_command(command)
    }

    pub fn submit_liveness_command_for_test(
        &mut self,
        command: ControlPlaneCommand,
    ) -> Result<ControlPlaneCommandResponse, ControlPlaneError> {
        self.submit_raft_liveness_command(command)
    }

    pub fn ensure_available_for_test(&self) -> Result<(), ControlPlaneError> {
        self.ensure_not_durably_poisoned()
    }

    pub fn poison_for_test(&self, message: String) {
        self.poison_durable_authority(message);
    }

    pub fn lease_horizon_authority_for_test(
        &self,
        raft_term: Option<u64>,
    ) -> Result<LeaseHorizonAuthorityBinding, ControlPlaneError> {
        self.authority_clock
            .as_ref()
            .ok_or_else(|| {
                ControlPlaneError::invariant_failure("test authority host has no authority clock")
            })?
            .lock()
            .expect("authority clock mutex should not be poisoned")
            .lease_horizon_authority_binding(raft_term)
    }

    pub fn set_after_heartbeat_commit_term_for_test(&self, raft_term: u64) {
        *self
            .after_heartbeat_commit_term
            .lock()
            .expect("heartbeat test hook mutex should not be poisoned") = Some(raft_term);
    }

    #[cfg(test)]
    pub(crate) fn set_after_unavailable_reconciliation_command_derived_for_test(
        &self,
        hook: Arc<dyn Fn() + Send + Sync>,
    ) {
        *self
            .after_unavailable_reconciliation_command_derived
            .lock()
            .expect("unavailable reconciliation test hook mutex should not be poisoned") =
            Some(hook);
    }

    #[cfg(test)]
    fn take_unavailable_reconciliation_command_derived_hook(
        &self,
    ) -> Option<Arc<dyn Fn() + Send + Sync>> {
        self.after_unavailable_reconciliation_command_derived
            .lock()
            .expect("unavailable reconciliation test hook mutex should not be poisoned")
            .take()
    }

    #[cfg(test)]
    fn set_after_unavailable_reconciliation_time_sampled_for_test(
        &self,
        hook: Arc<dyn Fn(u64) + Send + Sync>,
    ) {
        *self
            .after_unavailable_reconciliation_time_sampled
            .lock()
            .expect(
                "unavailable reconciliation time-sampled test hook mutex should not be poisoned",
            ) = Some(hook);
    }

    #[cfg(test)]
    fn run_after_unavailable_reconciliation_time_sampled_hook(&self, now_ms: u64) {
        let hook = self
            .after_unavailable_reconciliation_time_sampled
            .lock()
            .expect(
                "unavailable reconciliation time-sampled test hook mutex should not be poisoned",
            )
            .take();
        if let Some(hook) = hook {
            hook(now_ms);
        }
    }

    pub fn shares_lifecycle_for_test(&self, other: &Self) -> bool {
        self.authority_clock
            .as_ref()
            .zip(other.authority_clock.as_ref())
            .is_some_and(|(left, right)| Arc::ptr_eq(left, right))
            && self
                .durability
                .as_ref()
                .zip(other.durability.as_ref())
                .is_some_and(|(left, right)| left.shares_lifecycle_for_test(right))
    }
}

impl ControlPlaneRuntimeMapSource for ControlPlaneRaftAuthorityHost {
    fn runtime_map_snapshot(
        &self,
        authority_now_ms: u64,
    ) -> Result<ClusterRuntimeMapSnapshot, ControlPlaneError> {
        self.ensure_not_durably_poisoned()?;
        let authority_now_ms = self.authority_now_ms(authority_now_ms)?;
        let snapshot = self.block_on(
            self.authority
                .linearized_runtime_map_snapshot(authority_now_ms),
        )?;
        self.checkpoint_successful_linearized_read()?;
        Ok(snapshot)
    }

    fn runtime_map_status(
        &self,
        authority_now_ms: u64,
    ) -> Result<ControlPlaneRuntimeMapStatus, ControlPlaneError> {
        self.ensure_not_durably_poisoned()?;
        let authority_now_ms = self.authority_now_ms(authority_now_ms)?;
        let status = self.block_on(
            self.authority
                .linearized_runtime_map_status(authority_now_ms),
        )?;
        self.checkpoint_successful_linearized_read()?;
        Ok(status)
    }

    fn runtime_map_diagnostics_snapshot(
        &self,
        authority_now_ms: u64,
    ) -> Result<ControlPlaneRuntimeMapDiagnosticSnapshot, ControlPlaneError> {
        self.ensure_not_durably_poisoned()?;
        let authority_now_ms = self.authority_now_ms(authority_now_ms)?;
        let diagnostics = self.block_on(
            self.authority
                .linearized_runtime_map_diagnostics_snapshot(authority_now_ms),
        )?;
        self.checkpoint_successful_linearized_read()?;
        Ok(diagnostics)
    }

    fn serving_pg_runtime_map_snapshot(
        &self,
        pg_id: PgId,
        authority_now_ms: u64,
    ) -> Result<ClusterRuntimeMapSnapshot, ControlPlaneError> {
        self.ensure_not_durably_poisoned()?;
        let authority_now_ms = self.authority_now_ms(authority_now_ms)?;
        let snapshot = self.block_on(
            self.authority
                .linearized_serving_pg_runtime_map_snapshot(pg_id, authority_now_ms),
        )?;
        self.checkpoint_successful_linearized_read()?;
        Ok(snapshot)
    }
}

impl ControlPlaneHeartbeatRuntimeMapSource for ControlPlaneRaftAuthorityHost {
    fn refresh_node_heartbeat(
        &mut self,
        heartbeat: NodeHeartbeat,
        authority_now_ms: u64,
    ) -> Result<ControlPlaneHeartbeatRefresh, ControlPlaneError> {
        let (authority_now_ms, lease_horizon_authority) =
            self.authority_time_and_lease_horizon_binding(authority_now_ms)?;
        let node_id = heartbeat.node_id;
        let requested_observed_epoch = heartbeat.observed_epoch;
        let requested_lease_duration_ms = heartbeat.requested_lease_duration_ms;
        let pre_record_snapshot = self.current_snapshot()?;
        let previous_observed_epoch = pre_record_snapshot
            .node(node_id)
            .and_then(|node| node.last_observed_epoch());
        let carries_peering_evidence = heartbeat
            .pg_observations
            .iter()
            .any(|observation| observation.state == PgState::Peering);
        let lease_deadline_ms = pre_record_snapshot.heartbeat_lease_deadline(
            node_id,
            authority_now_ms,
            requested_lease_duration_ms,
        )?;
        let pre_record_epoch = pre_record_snapshot.cluster_epoch();
        let command = ControlPlaneCommand::RecordNodeHeartbeat {
            heartbeat,
            heartbeat_at_ms: authority_now_ms,
            lease_deadline_ms,
            lease_horizon_authority,
        };
        let mut volatile_snapshot = if carries_peering_evidence {
            None
        } else {
            self.block_on(self.authority.try_apply_volatile_heartbeat(command.clone()))?
        };
        if volatile_snapshot.as_ref().is_some_and(|snapshot| {
            snapshot
                .ready_pg_peering_completions(authority_now_ms)
                .is_ok_and(|ready| !ready.is_empty())
        }) {
            volatile_snapshot = None;
        }
        if volatile_snapshot.is_none() {
            self.submit_raft_liveness_command(command)?;
        }
        #[cfg(any(test, feature = "test-hooks"))]
        let post_commit_term_override = if volatile_snapshot.is_none() {
            self.run_after_heartbeat_commit_hook()
        } else {
            None
        };
        let snapshot = match volatile_snapshot {
            Some(snapshot) => snapshot,
            None => self.current_snapshot()?,
        };
        if let Some(expected_authority) = lease_horizon_authority {
            #[cfg(any(test, feature = "test-hooks"))]
            let current_authority = match post_commit_term_override {
                Some(current_term) => {
                    self.authority_time_and_lease_horizon_binding_for_term(current_term)?
                        .1
                }
                None => {
                    self.authority_time_and_lease_horizon_binding(authority_now_ms)?
                        .1
                }
            };
            #[cfg(not(any(test, feature = "test-hooks")))]
            let current_authority = self
                .authority_time_and_lease_horizon_binding(authority_now_ms)?
                .1;
            if current_authority != Some(expected_authority) {
                return Err(ControlPlaneError::LeaseGrantHorizonAuthorityTermMismatch {
                    authority_term: expected_authority.raft_term(),
                    committed_term: current_authority.and_then(|authority| authority.raft_term()),
                });
            }
            if !snapshot.lease_grant_horizon_covers(expected_authority, lease_deadline_ms) {
                return Err(ControlPlaneError::SnapshotInvariantViolation {
                    context: "committed Raft heartbeat horizon does not cover its lease",
                    message: format!(
                        "lease deadline {lease_deadline_ms} is outside the committed horizon"
                    ),
                });
            }
        }
        let mut lease = snapshot.heartbeat_lease_after_record(
            node_id,
            requested_observed_epoch,
            pre_record_epoch,
            lease_deadline_ms,
            authority_now_ms,
        )?;
        let ready = snapshot.ready_pg_peering_completions(authority_now_ms)?;
        if !ready.is_empty() {
            self.submit_raft_liveness_command(ControlPlaneCommand::CompleteReadyPgPeerings {
                ready_at_ms: authority_now_ms,
                ready,
            })?;
            lease = self
                .current_snapshot()?
                .current_heartbeat_lease_for_node(node_id, authority_now_ms)?;
        }
        let current_snapshot = self.current_snapshot()?;
        let current_epoch = current_snapshot.cluster_epoch();
        let observed_epoch = [Some(requested_observed_epoch), previous_observed_epoch]
            .into_iter()
            .flatten()
            .filter(|observed_epoch| *observed_epoch <= current_epoch)
            .max()
            .unwrap_or(requested_observed_epoch);
        let runtime_map = self
            .current_snapshot()?
            .runtime_map_for_storage_node_refresh(authority_now_ms, node_id, observed_epoch)?;
        Ok(ControlPlaneHeartbeatRefresh::new(
            lease,
            runtime_map,
            pre_record_epoch,
        ))
    }
}

impl ControlPlaneAdmin for ControlPlaneRaftAuthorityHost {
    fn authority_clock_context(
        &self,
    ) -> Result<ControlPlaneAuthorityClockContext, ControlPlaneError> {
        self.ensure_not_durably_poisoned()?;
        self.block_on(self.authority.authority_clock_context())
    }

    fn set_pg_acting_set(
        &mut self,
        pg_id: PgId,
        acting_set: Vec<NodeId>,
    ) -> Result<ClusterControlSnapshot, ControlPlaneError> {
        self.submit_raft_command(ControlPlaneCommand::SetPgActingSet { pg_id, acting_set })?;
        self.current_snapshot()
    }

    fn begin_unavailable_pg_placement_transition(
        &mut self,
        pg_id: PgId,
        unavailable_node_id: NodeId,
        begin_at_ms: u64,
    ) -> Result<ClusterControlSnapshot, ControlPlaneError> {
        self.submit_raft_command_derived(|current| {
            current.begin_unavailable_pg_placement_transition_command(
                pg_id,
                unavailable_node_id,
                authority_time_not_before_snapshot(current, begin_at_ms),
            )
        })?;
        self.current_snapshot()
    }

    fn complete_unavailable_pg_placement_transition(
        &mut self,
        work: &UnavailablePgReconciliationWork,
        ready_at_ms: u64,
    ) -> Result<ClusterControlSnapshot, ControlPlaneError> {
        self.submit_raft_command_derived(|current| {
            current.complete_unavailable_pg_placement_transition_command(
                work,
                authority_time_not_before_snapshot(current, ready_at_ms),
            )
        })?;
        self.current_snapshot()
    }

    fn apply_metadata_transfer_staging_evidence_page(
        &mut self,
        operation_payload: Vec<u8>,
        page_digest: [u8; 32],
    ) -> Result<Vec<u8>, ControlPlaneError> {
        let response = self.submit_raft_command(
            ControlPlaneCommand::ApplyMetadataTransferStagingEvidencePage {
                operation_payload,
                page_digest,
            },
        )?;
        let ControlPlaneCommandResponse::ApplyMetadataTransferStagingEvidencePage { apply_receipt } =
            response
        else {
            unreachable!("staging evidence command returned the wrong response");
        };
        Ok(apply_receipt)
    }

    fn fence_pg_for_metadata_transfer(
        &mut self,
        pg_id: PgId,
    ) -> Result<ClusterControlSnapshot, ControlPlaneError> {
        Ok(self
            .fence_pg_for_metadata_transfer_with_source_lease(pg_id)?
            .into_parts()
            .0)
    }

    fn fence_pg_for_metadata_transfer_with_source_lease(
        &mut self,
        pg_id: PgId,
    ) -> Result<FencedPgMetadataTransferSnapshot, ControlPlaneError> {
        let response =
            self.submit_raft_command(ControlPlaneCommand::FencePgForMetadataTransfer {
                pg_id,
                source_primary_lease_deadline_ms: None,
                lease_horizon_authority: None,
                unavailable_transition: None,
            })?;
        let ControlPlaneCommandResponse::FencePgForMetadataTransfer {
            source_primary_lease_deadline_ms,
        } = response
        else {
            unreachable!("metadata transfer fence command returned the wrong response");
        };
        Ok(FencedPgMetadataTransferSnapshot::new(
            self.current_snapshot()?,
            source_primary_lease_deadline_ms,
        ))
    }

    fn fence_unavailable_pg_transition_with_source_lease(
        &mut self,
        binding: crate::control_plane::UnavailablePgTransitionMutationBinding,
    ) -> Result<FencedPgMetadataTransferSnapshot, ControlPlaneError> {
        let response =
            self.submit_raft_command(ControlPlaneCommand::FencePgForMetadataTransfer {
                pg_id: binding.pg_id(),
                source_primary_lease_deadline_ms: None,
                lease_horizon_authority: None,
                unavailable_transition: Some(binding),
            })?;
        let ControlPlaneCommandResponse::FencePgForMetadataTransfer {
            source_primary_lease_deadline_ms,
        } = response
        else {
            unreachable!("metadata transfer fence command returned the wrong response");
        };
        Ok(FencedPgMetadataTransferSnapshot::new(
            self.current_snapshot()?,
            source_primary_lease_deadline_ms,
        ))
    }

    fn set_pg_acting_set_with_metadata_transfer(
        &mut self,
        pg_id: PgId,
        acting_set: Vec<NodeId>,
        transfer: PgMetadataTransferProof,
        expected_destination_epoch: ClusterEpoch,
    ) -> Result<ClusterControlSnapshot, ControlPlaneError> {
        self.submit_raft_command(ControlPlaneCommand::SetPgActingSetWithMetadataTransfer {
            pg_id,
            acting_set,
            transfer,
            expected_destination_epoch,
            unavailable_transition: None,
        })?;
        self.current_snapshot()
    }

    fn install_unavailable_pg_transition_metadata_transfer(
        &mut self,
        binding: crate::control_plane::UnavailablePgTransitionMutationBinding,
        transfer: PgMetadataTransferProof,
        expected_destination_epoch: ClusterEpoch,
    ) -> Result<ClusterControlSnapshot, ControlPlaneError> {
        self.submit_raft_command(ControlPlaneCommand::SetPgActingSetWithMetadataTransfer {
            pg_id: binding.pg_id(),
            acting_set: binding.destination_acting_set().to_vec(),
            transfer,
            expected_destination_epoch,
            unavailable_transition: Some(binding),
        })?;
        self.current_snapshot()
    }

    fn transfer_raft_leadership_to(
        &mut self,
        node_id: ControlPlaneRaftNodeId,
    ) -> Result<(), ControlPlaneError> {
        self.ensure_not_durably_poisoned()?;
        self.block_on(self.authority.transfer_leadership_to(node_id))
    }

    fn trigger_raft_snapshot_and_purge(&mut self) -> Result<Option<u64>, ControlPlaneError> {
        self.ensure_not_durably_poisoned()?;
        let snapshot_log_id = self.block_on(self.authority.trigger_snapshot_applied())?;
        if let Err(error) = self.store_durable_restart_artifact() {
            self.poison_durable_authority(format!(
                "OpenRaft control-plane durability checkpoint failed before a snapshot purge; \
                 refusing to serve until restart: {error}"
            ));
            return Err(error);
        }
        let Some(snapshot_log_id) = snapshot_log_id else {
            return Ok(None);
        };
        let purge_result =
            self.block_on(self.authority.purge_log_through_snapshot(snapshot_log_id));
        if let Err(error) = self.store_durable_restart_artifact() {
            self.poison_durable_authority(format!(
                "OpenRaft control-plane durability checkpoint failed after a snapshot purge \
                 attempt; refusing to serve until restart: {error}"
            ));
            return Err(error);
        }
        purge_result?;
        Ok(Some(snapshot_log_id.index()))
    }

    fn trigger_raft_election(&mut self) -> Result<(), ControlPlaneError> {
        self.ensure_not_durably_poisoned()?;
        self.block_on(
            self.authority
                .trigger_pre_vote_election_until_serving(Duration::from_secs(10)),
        )?;
        if let Err(error) = self.store_durable_restart_artifact() {
            self.poison_durable_authority(format!(
                "OpenRaft control-plane durability checkpoint failed after an election trigger; \
                 refusing to serve until restart: {error}"
            ));
            return Err(error);
        }
        Ok(())
    }
}

fn authority_time_not_before_snapshot(
    snapshot: &ClusterControlSnapshot,
    sampled_now_ms: u64,
) -> u64 {
    snapshot
        .max_committed_timestamp_ms()
        .map_or(sampled_now_ms, |committed| committed.max(sampled_now_ms))
}

fn resolve_derived_preparation<T, R>(
    prepared: Option<T>,
    submit_result: Result<R, ControlPlaneError>,
    missing_preparation_diagnostic: &'static str,
) -> Result<(T, Result<R, ControlPlaneError>), ControlPlaneError> {
    match prepared {
        Some(prepared) => Ok((prepared, submit_result)),
        None => match submit_result {
            Err(error) => Err(error),
            Ok(_) => Err(ControlPlaneError::invariant_failure(
                missing_preparation_diagnostic,
            )),
        },
    }
}

fn block_on_control_plane_raft<F: Future>(runtime: &Handle, future: F) -> F::Output {
    if Handle::try_current().is_ok() {
        tokio::task::block_in_place(|| runtime.block_on(future))
    } else {
        runtime.block_on(future)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::PathBuf;
    use std::sync::mpsc;
    use std::thread;

    struct ReconciliationCommandRelease(Option<mpsc::SyncSender<()>>);

    #[test]
    fn pre_derivation_reconciliation_submission_error_is_preserved() {
        let error = resolve_derived_preparation::<(), ()>(
            None,
            Err(ControlPlaneError::AuthorityClockLeadershipChanged {
                established_term: Some(7),
                current_term: 8,
            }),
            "missing preparation",
        )
        .unwrap_err();

        assert!(matches!(
            error,
            ControlPlaneError::AuthorityClockLeadershipChanged {
                established_term: Some(7),
                current_term: 8,
            }
        ));
    }

    impl ReconciliationCommandRelease {
        fn release(&mut self) {
            if let Some(release) = self.0.take() {
                let _ = release.send(());
            }
        }
    }

    impl Drop for ReconciliationCommandRelease {
        fn drop(&mut self) {
            self.release();
        }
    }

    fn wall_clock_sample_after(timestamp_ms: u64) -> u64 {
        let deadline = std::time::Instant::now() + Duration::from_secs(2);
        loop {
            let now_ms = crate::clock::current_time_millis();
            if now_ms > timestamp_ms {
                return now_ms;
            }
            assert!(
                std::time::Instant::now() < deadline,
                "wall clock did not advance beyond the paused authority sample"
            );
            thread::sleep(Duration::from_millis(1));
        }
    }

    fn assert_committed_timestamp_advanced_past(
        host: &ControlPlaneRaftAuthorityHost,
        previous_timestamp_ms: u64,
    ) -> u64 {
        let committed_timestamp_ms = host
            .current_snapshot_for_test()
            .unwrap()
            .max_committed_timestamp_ms()
            .expect("durable heartbeat must publish a committed timestamp");
        assert!(
            committed_timestamp_ms > previous_timestamp_ms,
            "durable heartbeat did not advance committed authority time: \
             committed={committed_timestamp_ms} previous={previous_timestamp_ms}"
        );
        committed_timestamp_ms
    }

    fn run_reconciliation_across_durable_heartbeat_races<T: Send>(
        host: &mut ControlPlaneRaftAuthorityHost,
        reconcile: impl FnOnce(&mut ControlPlaneRaftAuthorityHost) -> T + Send,
        commit_before_derivation: impl FnOnce(&mut ControlPlaneRaftAuthorityHost, u64),
        wait_after_derivation: impl FnOnce(&mut ControlPlaneRaftAuthorityHost, u64) + Send,
    ) -> T {
        let (sampled_tx, sampled_rx) = mpsc::sync_channel(1);
        let (sampled_release_tx, sampled_release_rx) = mpsc::sync_channel(1);
        let sampled_release_rx = Arc::new(Mutex::new(sampled_release_rx));
        host.set_after_unavailable_reconciliation_time_sampled_for_test(Arc::new({
            let sampled_release_rx = Arc::clone(&sampled_release_rx);
            move |now_ms| {
                sampled_tx.send(now_ms).unwrap();
                sampled_release_rx
                    .lock()
                    .unwrap()
                    .recv_timeout(Duration::from_secs(2))
                    .expect("reconciliation time sample was not released");
            }
        }));
        let (derived_tx, derived_rx) = mpsc::sync_channel(1);
        let (release_tx, release_rx) = mpsc::sync_channel(1);
        let release_rx = Arc::new(Mutex::new(release_rx));
        host.set_after_unavailable_reconciliation_command_derived_for_test(Arc::new({
            let release_rx = Arc::clone(&release_rx);
            move || {
                derived_tx.send(()).unwrap();
                release_rx
                    .lock()
                    .unwrap()
                    .recv_timeout(Duration::from_secs(2))
                    .expect("reconciliation command derivation was not released");
            }
        }));
        let mut committed_heartbeat_host = host.clone();
        let mut waiting_heartbeat_host = host.clone();
        let authority = Arc::clone(&host.authority);
        thread::scope(|scope| {
            let mut sampled_release = ReconciliationCommandRelease(Some(sampled_release_tx));
            let mut release = ReconciliationCommandRelease(Some(release_tx));
            let reconciliation = scope.spawn(|| reconcile(host));
            let sampled_now_ms = sampled_rx
                .recv_timeout(Duration::from_secs(2))
                .expect("reconciliation authority time was not sampled");
            let heartbeat_at_ms = wall_clock_sample_after(sampled_now_ms);
            commit_before_derivation(&mut committed_heartbeat_host, heartbeat_at_ms);
            let first_committed_timestamp_ms =
                assert_committed_timestamp_advanced_past(&committed_heartbeat_host, sampled_now_ms);
            sampled_release.release();
            derived_rx
                .recv_timeout(Duration::from_secs(2))
                .expect("reconciliation command was not derived");
            let (heartbeat_arrived_tx, heartbeat_arrived_rx) = mpsc::sync_channel(1);
            authority.set_before_heartbeat_update_gate_hook_for_test(Arc::new(move || {
                heartbeat_arrived_tx.send(()).unwrap();
            }));
            let (heartbeat_done_tx, heartbeat_done_rx) = mpsc::sync_channel(1);
            let renewing_heartbeat = scope.spawn(move || {
                let heartbeat_at_ms = wall_clock_sample_after(first_committed_timestamp_ms);
                wait_after_derivation(&mut waiting_heartbeat_host, heartbeat_at_ms);
                heartbeat_done_tx.send(()).unwrap();
                waiting_heartbeat_host
            });
            heartbeat_arrived_rx
                .recv_timeout(Duration::from_secs(2))
                .expect("concurrent heartbeat did not reach the update gate");
            assert!(matches!(
                heartbeat_done_rx.try_recv(),
                Err(mpsc::TryRecvError::Empty)
            ));
            release.release();
            let result = reconciliation
                .join()
                .expect("reconciliation thread panicked");
            let waiting_heartbeat_host = renewing_heartbeat
                .join()
                .expect("concurrent heartbeat thread panicked");
            heartbeat_done_rx
                .recv_timeout(Duration::from_secs(2))
                .expect("concurrent heartbeat did not complete after command submission");
            assert_committed_timestamp_advanced_past(
                &waiting_heartbeat_host,
                first_committed_timestamp_ms,
            );
            result
        })
    }

    fn run_admin_derivation_after_durable_heartbeat<T: Send>(
        host: &mut ControlPlaneRaftAuthorityHost,
        command_time_ms: u64,
        operation: impl FnOnce(&mut ControlPlaneRaftAuthorityHost) -> T + Send,
        renew: impl FnOnce(&mut ControlPlaneRaftAuthorityHost, u64),
    ) -> T {
        let (arrived_tx, arrived_rx) = mpsc::sync_channel(1);
        let (release_tx, release_rx) = mpsc::sync_channel(1);
        let release_rx = Arc::new(Mutex::new(release_rx));
        host.authority
            .set_before_heartbeat_update_gate_hook_for_test(Arc::new({
                let release_rx = Arc::clone(&release_rx);
                move || {
                    arrived_tx.send(()).unwrap();
                    release_rx
                        .lock()
                        .unwrap()
                        .recv_timeout(Duration::from_secs(2))
                        .expect("admin command gate arrival was not released");
                }
            }));
        let mut heartbeat_host = host.clone();
        thread::scope(|scope| {
            let mut release = ReconciliationCommandRelease(Some(release_tx));
            let command = scope.spawn(|| operation(host));
            arrived_rx
                .recv_timeout(Duration::from_secs(2))
                .expect("admin command did not reach the update gate");
            let heartbeat_at_ms = wall_clock_sample_after(command_time_ms);
            renew(&mut heartbeat_host, heartbeat_at_ms);
            assert_committed_timestamp_advanced_past(&heartbeat_host, command_time_ms);
            release.release();
            command.join().expect("admin command thread panicked")
        })
    }

    fn runtime() -> tokio::runtime::Runtime {
        tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .build()
            .expect("test runtime should build")
    }

    #[test]
    fn durable_raft_host_poll_batches_grace_expired_unavailable_pg_transitions() {
        let tmp = test_util::tempdir();
        let artifact_path = tmp.path().join("authority.state");
        let runtime = runtime();
        let authority = Arc::new(
            runtime
                .block_on(
                    ControlPlaneRaftAuthority::new_experimental_single_node_durable(
                        "unavailable-pg-reconciliation-raft-host",
                        1,
                        &artifact_path,
                    ),
                )
                .unwrap(),
        );
        runtime
            .block_on(authority.initialize_single_node_membership(1))
            .unwrap();
        runtime
            .block_on(authority.wait_for_current_leader(
                1,
                Duration::from_secs(1),
                "unavailable-PG reconciliation test authority startup",
            ))
            .unwrap();
        runtime.block_on(async {
            let deadline = std::time::Instant::now() + Duration::from_secs(5);
            loop {
                match authority.confirmed_linearized_authority_status().await {
                    Ok(_) => break,
                    Err(error)
                        if error.is_control_plane_leader_routing_rejection()
                            && std::time::Instant::now() < deadline =>
                    {
                        tokio::time::sleep(Duration::from_millis(1)).await;
                    }
                    Err(error) => {
                        panic!(
                            "unavailable-PG reconciliation test authority did not become serving: {error:?}"
                        );
                    }
                }
            }
        });
        let mut host = ControlPlaneRaftAuthorityHost::start_durable(
            runtime.handle().clone(),
            Arc::clone(&authority),
        )
        .unwrap();
        let pg_id = PgId::new(7);
        let admin_pg_id = PgId::new(8);
        let nodes = (1..=5)
            .map(|node_id| {
                (
                    NodeId::new(node_id),
                    tmp.path()
                        .join(format!("node-{node_id}.sock"))
                        .display()
                        .to_string(),
                )
            })
            .collect::<Vec<_>>();
        let source_acting_set = vec![NodeId::new(1), NodeId::new(2), NodeId::new(3)];
        let pgs = vec![
            (pg_id, source_acting_set.clone()),
            (admin_pg_id, source_acting_set),
        ];
        let topology =
            crate::control_plane::InitialClusterTopologyCertificate::new_for_bootstrap_map(
                7,
                [0x4d; crate::control_plane::CONTROL_PLANE_TOPOLOGY_DIGEST_LEN],
                vec![1],
                &nodes,
                &pgs,
                crate::control_plane::test_certified_storage_placement_policy(
                    (1..=5).map(NodeId::new),
                    3,
                    2_000,
                ),
            )
            .unwrap();
        host.submit_command_for_test(ControlPlaneCommand::BootstrapCertifiedInitialClusterMap {
            nodes: nodes.clone(),
            pg_acting_sets: pgs,
            topology,
        })
        .unwrap();

        let heartbeat = |host: &mut ControlPlaneRaftAuthorityHost,
                         node_id: u32,
                         lease_ms: u64,
                         state: Option<PgState>,
                         heartbeat_at_ms: u64,
                         observed_pg_ids: &[PgId]| {
            for _ in 0..4 {
                let observed_epoch = host.current_snapshot_for_test().unwrap().cluster_epoch();
                let refresh = match host.refresh_node_heartbeat(
                    NodeHeartbeat {
                        node_id: NodeId::new(node_id),
                        node_incarnation: 1,
                        endpoint: nodes[usize::try_from(node_id - 1).unwrap()].1.clone(),
                        observed_epoch,
                        requested_lease_duration_ms: lease_ms,
                        cluster_map_history_route_scan_generation: std::num::NonZeroU64::new(1)
                            .unwrap(),
                        cluster_map_history_route_references: Default::default(),
                        pg_observations: state
                            .into_iter()
                            .flat_map(|state| {
                                observed_pg_ids.iter().copied().map(move |pg_id| {
                                    crate::control_plane::NodePgHeartbeatObservation {
                                        pg_id,
                                        state,
                                        metadata_proof:
                                            crate::control_plane::PgMetadataProof::empty(),
                                        pending_metadata_command: None,
                                    }
                                })
                            })
                            .collect(),
                    },
                    heartbeat_at_ms,
                ) {
                    Ok(refresh) => refresh,
                    Err(ControlPlaneError::PgPrimaryObservationNotActive { .. })
                        if state == Some(PgState::Active) =>
                    {
                        continue;
                    }
                    Err(ControlPlaneError::PgPrimaryObservationNotActive { .. })
                        if state == Some(PgState::Peering) =>
                    {
                        return;
                    }
                    Err(error) => {
                        panic!("node {node_id} {state:?} heartbeat failed: {error:?}")
                    }
                };
                if refresh.lease().serving() {
                    return;
                }
            }
            panic!("node {node_id} did not receive a serving lease");
        };
        heartbeat(
            &mut host,
            2,
            3_000,
            Some(PgState::Peering),
            crate::clock::current_time_millis(),
            &[pg_id, admin_pg_id],
        );
        heartbeat(
            &mut host,
            2,
            3_000,
            Some(PgState::Active),
            crate::clock::current_time_millis(),
            &[pg_id, admin_pg_id],
        );
        heartbeat(
            &mut host,
            1,
            50,
            Some(PgState::Active),
            crate::clock::current_time_millis(),
            &[pg_id, admin_pg_id],
        );
        heartbeat(
            &mut host,
            3,
            10_000,
            Some(PgState::Active),
            crate::clock::current_time_millis(),
            &[pg_id, admin_pg_id],
        );
        heartbeat(
            &mut host,
            4,
            10_000,
            None,
            crate::clock::current_time_millis(),
            &[pg_id, admin_pg_id],
        );
        heartbeat(
            &mut host,
            5,
            10_000,
            None,
            crate::clock::current_time_millis(),
            &[pg_id, admin_pg_id],
        );
        thread::sleep(Duration::from_millis(60));
        host.expire_heartbeat_leases(crate::clock::current_time_millis())
            .unwrap();
        thread::sleep(Duration::from_millis(
            crate::control_plane::CONTROL_PLANE_AUTHORITY_CLOCK_SKEW_BUDGET_MS + 10,
        ));
        for (node_id, lease_ms, state) in [
            (2, 3_000, None),
            (3, 10_000, None),
            (4, 10_000, None),
            (5, 10_000, None),
            (2, 3_000, Some(PgState::Peering)),
            (3, 10_000, Some(PgState::Peering)),
            (2, 3_000, Some(PgState::Active)),
            (3, 10_000, Some(PgState::Active)),
            (4, 10_000, None),
            (5, 10_000, None),
        ] {
            heartbeat(
                &mut host,
                node_id,
                lease_ms,
                state,
                crate::clock::current_time_millis(),
                &[pg_id, admin_pg_id],
            );
        }
        let reactivated = host.current_snapshot_for_test().unwrap();
        assert_eq!(
            reactivated.pg(pg_id).unwrap().state(),
            PgState::Active,
            "surviving primary must reactivate the degraded source before reconciliation"
        );
        let grace_cutoff_ms = reactivated
            .unavailable_node_observation(NodeId::new(1))
            .unwrap()
            .observed_at_ms()
            .saturating_add(2_000);
        thread::sleep(Duration::from_millis(
            grace_cutoff_ms
                .saturating_sub(crate::clock::current_time_millis())
                .saturating_add(2),
        ));

        let mut cursor = UnavailablePgReconciliationCursor::start();
        let work = run_reconciliation_across_durable_heartbeat_races(
            &mut host,
            |host| {
                host.poll_unavailable_pg_reconciliation_batch(
                    &mut cursor,
                    crate::clock::current_time_millis(),
                )
                .unwrap()
            },
            |host, now_ms| {
                heartbeat(
                    host,
                    3,
                    10_000,
                    Some(PgState::Peering),
                    now_ms,
                    &[pg_id, admin_pg_id],
                )
            },
            |host, now_ms| {
                heartbeat(
                    host,
                    3,
                    10_000,
                    Some(PgState::Peering),
                    now_ms,
                    &[pg_id, admin_pg_id],
                )
            },
        );
        assert!(work.rejected.is_empty());
        assert_eq!(work.work.len(), 2);
        assert_eq!(
            work.work
                .iter()
                .map(UnavailablePgReconciliationWork::pg_id)
                .collect::<Vec<_>>(),
            vec![pg_id, admin_pg_id]
        );
        assert_eq!(
            work.work[0].transition_epoch(),
            work.work[1].transition_epoch()
        );
        let work = work
            .work
            .into_iter()
            .find(|work| work.pg_id() == pg_id)
            .expect("batch must contain the data PG transition");
        assert_eq!(
            host.current_snapshot_for_test()
                .unwrap()
                .pg(pg_id)
                .unwrap()
                .state(),
            PgState::Peering
        );
        assert_eq!(work.pg_id(), pg_id);
        assert_eq!(
            work.stage(),
            UnavailablePgReconciliationStage::MetadataTransfer
        );
        assert_eq!(
            work.destination_acting_set(),
            &[NodeId::new(4), NodeId::new(2), NodeId::new(3)]
        );
        let snapshot = host.current_snapshot_for_test().unwrap();
        let transition = snapshot
            .unavailable_pg_placement_transition(pg_id)
            .expect("Raft poll must durably install the exact transition");
        assert_eq!(transition.transition_epoch(), work.transition_epoch());
        assert_eq!(
            transition.destination_acting_set(),
            work.destination_acting_set()
        );

        let source_lease_deadline_ms = snapshot
            .node(NodeId::new(2))
            .unwrap()
            .lease_deadline_ms()
            .unwrap();
        thread::sleep(Duration::from_millis(
            source_lease_deadline_ms
                .saturating_sub(crate::clock::current_time_millis())
                .saturating_add(
                    crate::control_plane::CONTROL_PLANE_AUTHORITY_CLOCK_SKEW_BUDGET_MS + 10,
                ),
        ));

        let transfer = PgMetadataTransferProof::new(
            snapshot.cluster_epoch(),
            crate::control_plane::PgMetadataProof::empty(),
        );
        let destination_epoch = ClusterEpoch::new(snapshot.cluster_epoch().get() + 1).unwrap();
        host.install_unavailable_pg_transition_metadata_transfer(
            work.mutation_binding().clone(),
            transfer,
            destination_epoch,
        )
        .unwrap();
        for &node_id in work.destination_acting_set() {
            heartbeat(
                &mut host,
                node_id.as_u32(),
                10_000,
                Some(PgState::Peering),
                crate::clock::current_time_millis(),
                &[pg_id],
            );
        }
        let readiness_work = UnavailablePgReconciliationWork::from_transition(
            host.current_snapshot_for_test()
                .unwrap()
                .unavailable_pg_placement_transition(pg_id)
                .unwrap(),
            UnavailablePgReconciliationStage::PayloadReadiness,
        );
        assert!(run_reconciliation_across_durable_heartbeat_races(
            &mut host,
            |host| {
                host.complete_unavailable_pg_reconciliation(
                    &readiness_work,
                    crate::clock::current_time_millis(),
                )
                .unwrap()
            },
            |host, now_ms| { heartbeat(host, 3, 10_000, Some(PgState::Peering), now_ms, &[pg_id]) },
            |host, now_ms| { heartbeat(host, 3, 10_000, Some(PgState::Peering), now_ms, &[pg_id]) },
        ));
        assert_eq!(
            host.current_snapshot_for_test()
                .unwrap()
                .pg(pg_id)
                .unwrap()
                .state(),
            PgState::Active
        );

        for (node_id, lease_ms, state) in [
            (2, 3_000, Some(PgState::Peering)),
            (3, 10_000, Some(PgState::Peering)),
            (2, 3_000, Some(PgState::Active)),
            (3, 10_000, Some(PgState::Active)),
            (5, 10_000, None),
        ] {
            heartbeat(
                &mut host,
                node_id,
                lease_ms,
                state,
                crate::clock::current_time_millis(),
                state.map_or(&[], |_| std::slice::from_ref(&admin_pg_id)),
            );
        }
        let direct_begin_at_ms = crate::clock::current_time_millis();
        run_admin_derivation_after_durable_heartbeat(
            &mut host,
            direct_begin_at_ms,
            |host| {
                <ControlPlaneRaftAuthorityHost as ControlPlaneAdmin>::begin_unavailable_pg_placement_transition(
                    host,
                    admin_pg_id,
                    NodeId::new(1),
                    direct_begin_at_ms,
                )
                .unwrap()
            },
            |host, now_ms| {
                heartbeat(
                    host,
                    3,
                    10_000,
                    Some(PgState::Peering),
                    now_ms,
                    &[admin_pg_id],
                )
            },
        );
        let direct_begin_snapshot = host.current_snapshot_for_test().unwrap();
        assert_eq!(
            direct_begin_snapshot.pg(admin_pg_id).unwrap().state(),
            PgState::Peering
        );
        let direct_work = UnavailablePgReconciliationWork::from_transition(
            direct_begin_snapshot
                .unavailable_pg_placement_transition(admin_pg_id)
                .expect("direct admin begin must install the exact transition"),
            UnavailablePgReconciliationStage::MetadataTransfer,
        );
        let direct_source_lease_deadline_ms = direct_begin_snapshot
            .node(NodeId::new(2))
            .unwrap()
            .lease_deadline_ms()
            .unwrap();
        thread::sleep(Duration::from_millis(
            direct_source_lease_deadline_ms
                .saturating_sub(crate::clock::current_time_millis())
                .saturating_add(
                    crate::control_plane::CONTROL_PLANE_AUTHORITY_CLOCK_SKEW_BUDGET_MS + 10,
                ),
        ));
        host.install_unavailable_pg_transition_metadata_transfer(
            direct_work.mutation_binding().clone(),
            PgMetadataTransferProof::new(
                direct_begin_snapshot.cluster_epoch(),
                crate::control_plane::PgMetadataProof::empty(),
            ),
            ClusterEpoch::new(direct_begin_snapshot.cluster_epoch().get() + 1).unwrap(),
        )
        .unwrap();
        for &node_id in direct_work.destination_acting_set() {
            heartbeat(
                &mut host,
                node_id.as_u32(),
                10_000,
                Some(PgState::Peering),
                crate::clock::current_time_millis(),
                &[admin_pg_id],
            );
        }
        let direct_readiness_work = UnavailablePgReconciliationWork::from_transition(
            host.current_snapshot_for_test()
                .unwrap()
                .unavailable_pg_placement_transition(admin_pg_id)
                .unwrap(),
            UnavailablePgReconciliationStage::PayloadReadiness,
        );
        let direct_ready_at_ms = crate::clock::current_time_millis();
        host.current_snapshot_for_test()
            .unwrap()
            .complete_unavailable_pg_placement_transition_command(
                &direct_readiness_work,
                direct_ready_at_ms,
            )
            .expect("direct admin destination must be ready before the renewal race");
        run_admin_derivation_after_durable_heartbeat(
            &mut host,
            direct_ready_at_ms,
            |host| {
                <ControlPlaneRaftAuthorityHost as ControlPlaneAdmin>::complete_unavailable_pg_placement_transition(
                    host,
                    &direct_readiness_work,
                    direct_ready_at_ms,
                )
                .unwrap()
            },
            |host, now_ms| {
                heartbeat(
                    host,
                    3,
                    10_000,
                    Some(PgState::Peering),
                    now_ms,
                    &[admin_pg_id],
                )
            },
        );
        assert_eq!(
            host.current_snapshot_for_test()
                .unwrap()
                .pg(admin_pg_id)
                .unwrap()
                .state(),
            PgState::Active
        );

        drop(host);
        runtime.block_on(authority.shutdown()).unwrap();
    }

    #[test]
    fn durable_host_is_authority_derived_shared_and_redacted() {
        let tmp = test_util::tempdir();
        let artifact_path = tmp.path().join("authority.state");
        let secret_cluster_name = "secret-authority-host-cluster";
        let runtime = runtime();
        let authority = Arc::new(
            runtime
                .block_on(
                    ControlPlaneRaftAuthority::new_experimental_single_node_durable(
                        secret_cluster_name,
                        1,
                        &artifact_path,
                    ),
                )
                .expect("durable authority should initialize"),
        );
        let first = ControlPlaneRaftAuthorityHost::start_durable(
            runtime.handle().clone(),
            Arc::clone(&authority),
        )
        .expect("authority should issue its durable host");
        let second = ControlPlaneRaftAuthorityHost::start_durable(
            runtime.handle().clone(),
            Arc::clone(&authority),
        )
        .expect("repeated host issuance should succeed");
        assert!(
            first.shares_lifecycle_for_test(&second),
            "one authority must retain one clock and durability lifecycle"
        );

        let debug = format!("{first:?}");
        assert!(!debug.contains(secret_cluster_name), "{debug}");
        assert!(
            !debug.contains(artifact_path.to_string_lossy().as_ref()),
            "{debug}"
        );
        runtime
            .block_on(authority.shutdown())
            .expect("authority should shut down");
    }

    #[test]
    fn durable_host_uses_the_runtime_retained_by_preissued_durability() {
        let tmp = test_util::tempdir();
        let artifact_path = tmp.path().join("authority.state");
        let authority_runtime = runtime();
        let authority = Arc::new(
            authority_runtime
                .block_on(
                    ControlPlaneRaftAuthority::new_experimental_single_node_durable(
                        "crossed-runtime-authority-host",
                        1,
                        &artifact_path,
                    ),
                )
                .expect("durable authority should initialize"),
        );
        authority
            .durability_lifecycle(authority_runtime.handle().clone())
            .expect("durability should retain the authority runtime");

        let crossed_runtime = runtime();
        let host = ControlPlaneRaftAuthorityHost::start_durable(
            crossed_runtime.handle().clone(),
            Arc::clone(&authority),
        )
        .expect("host should derive the preissued durability runtime");
        drop(crossed_runtime);

        host.current_snapshot_for_test()
            .expect("host operations must remain on the retained authority runtime");
        host.store_restart_artifact_for_test()
            .expect("checkpointing must use the same retained runtime domain");
        authority_runtime
            .block_on(authority.shutdown())
            .expect("authority should shut down");
    }

    #[test]
    fn durable_host_rejects_an_authority_without_durable_state() {
        let runtime = runtime();
        let authority = Arc::new(
            runtime
                .block_on(
                    ControlPlaneRaftAuthority::new_experimental_single_node_in_memory(
                        "in-memory-authority-host",
                        1,
                    ),
                )
                .expect("in-memory authority should initialize"),
        );
        let error = ControlPlaneRaftAuthorityHost::start_durable(
            runtime.handle().clone(),
            Arc::clone(&authority),
        )
        .expect_err("durable host requires authority-owned durable state");
        assert!(matches!(error, ControlPlaneError::DurabilityFailure { .. }));
        runtime
            .block_on(authority.shutdown())
            .expect("authority should shut down");
    }

    #[test]
    fn restored_nonleading_clock_defers_term_binding_until_startup_converges() {
        let binding =
            ControlPlaneAuthorityClockCheckpointBinding::for_raft("restored-nonleading-clock", 1);
        let checkpoint =
            ControlPlaneAuthorityClockRestartCheckpoint::new(binding, 4, Some(1_000), 1_000, 50);
        let mut clock = ControlPlaneAuthorityClock::new_with_restart_checkpoint(
            Some(1_000),
            1_001,
            Some(51),
            Some(checkpoint),
        )
        .expect("valid restored clock should initialize");

        assert!(
            prepare_initial_raft_leadership_term(&mut clock, false, Some(3))
                .expect("non-leading preparation should defer"),
            "restored membership must not close term binding before convergence"
        );
        clock.bind_initial_raft_leadership_term(Some(4));
        clock
            .validate_raft_leadership_term(4)
            .expect("converged startup term should remain clock-authorized");
    }

    #[test]
    fn durable_host_rejects_unknown_and_unsupported_clock_checkpoint_formats_without_publication() {
        let tmp = test_util::tempdir();
        let artifact_path = tmp.path().join("authority.state");
        let runtime = runtime();
        let authority = Arc::new(
            runtime
                .block_on(
                    ControlPlaneRaftAuthority::new_experimental_single_node_durable(
                        "clock-checkpoint-format-boundary",
                        1,
                        &artifact_path,
                    ),
                )
                .expect("durable authority should initialize"),
        );
        authority
            .durability_lifecycle(runtime.handle().clone())
            .expect("authority should issue its durability lifecycle")
            .store_restart_artifact()
            .expect("initial restart artifact and clock checkpoint should persist");
        let artifact_before = std::fs::read(&artifact_path).unwrap();
        let mut checkpoint_path = artifact_path.as_os_str().to_os_string();
        checkpoint_path.push(".clock");
        let checkpoint_path = PathBuf::from(checkpoint_path);
        let current_checkpoint = std::fs::read(&checkpoint_path).unwrap();

        let mut hard_failures = Vec::new();
        let mut bad_magic = current_checkpoint.clone();
        bad_magic[0] ^= 0xff;
        reseal_crc64_suffix_for_test(&mut bad_magic);
        hard_failures.push((bad_magic, "checkpoint magic mismatch".to_owned()));
        for version in [1u16, 3u16] {
            let mut unsupported = current_checkpoint.clone();
            unsupported[8..10].copy_from_slice(&version.to_be_bytes());
            reseal_crc64_suffix_for_test(&mut unsupported);
            hard_failures.push((
                unsupported,
                format!("unsupported checkpoint version {version}"),
            ));
        }

        for (bytes, expected_message) in hard_failures {
            std::fs::write(&checkpoint_path, &bytes).unwrap();
            let error = ControlPlaneRaftAuthorityHost::start_durable(
                runtime.handle().clone(),
                Arc::clone(&authority),
            )
            .expect_err("hard checkpoint formats must prevent durable host publication");
            assert!(matches!(
                error,
                ControlPlaneError::AuthorityClockCheckpoint { message }
                    if message == expected_message
            ));
            assert!(authority.authority_host_lifecycle_slot().get().is_none());
            assert_eq!(std::fs::read(&artifact_path).unwrap(), artifact_before);
            assert_eq!(std::fs::read(&checkpoint_path).unwrap(), bytes);
        }

        runtime
            .block_on(authority.shutdown())
            .expect("authority should shut down");
    }

    fn reseal_crc64_suffix_for_test(bytes: &mut [u8]) {
        let checksum_offset = bytes.len() - std::mem::size_of::<u64>();
        let checksum = checksum::crc64::checksum(&bytes[..checksum_offset]);
        bytes[checksum_offset..].copy_from_slice(&checksum.to_be_bytes());
    }
}
