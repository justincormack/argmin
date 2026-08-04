use std::future::Future;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use placement::NodeId;
use tokio::runtime::Handle;

use crate::control_plane::{
    ClusterControlSnapshot, ClusterRuntimeMapSnapshot, ControlPlaneAdmin,
    ControlPlaneAuthorityClock, ControlPlaneAuthorityClockContext, ControlPlaneError,
    ControlPlaneHeartbeatRefresh, ControlPlaneHeartbeatRuntimeMapSource,
    ControlPlaneRpcResponsePublication, ControlPlaneRuntimeMapDiagnosticSnapshot,
    ControlPlaneRuntimeMapSource, ControlPlaneRuntimeMapStatus, FencedPgMetadataTransferSnapshot,
    LeaseHorizonAuthorityBinding, NodeHeartbeat, PgMetadataTransferProof,
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
}

pub(crate) struct DurableAuthorityHostLifecycle {
    runtime: Handle,
    durability: ControlPlaneRaftAuthorityDurability,
    authority_clock: Arc<Mutex<ControlPlaneAuthorityClock>>,
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
    pub(crate) fn start_durable(
        runtime: Handle,
        authority: Arc<ControlPlaneRaftAuthority>,
    ) -> Result<Self, ControlPlaneError> {
        if let Some(lifecycle) = authority
            .authority_host_lifecycle_slot()
            .get()
            .map(Arc::clone)
        {
            return Ok(Self::from_durable_lifecycle(authority, &lifecycle));
        }
        let durability = authority.durability_lifecycle(runtime)?;
        let runtime = durability.runtime();
        let restart_checkpoint =
            durability.load_authority_clock_restart_checkpoint_for_startup()?;
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
        if initial_status.local_leader() {
            authority_clock.bind_initial_raft_leadership_term(initial_status.current_term());
        } else if !authority.initialized_membership_in_process() {
            // A restored or joining follower must not use the fresh-cluster
            // first-term exception when it later becomes leader.
            authority_clock.bind_initial_raft_leadership_term(None);
        }
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
        Ok(Self::from_durable_lifecycle(authority, &lifecycle))
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

    fn runtime() -> tokio::runtime::Runtime {
        tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .build()
            .expect("test runtime should build")
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
}
