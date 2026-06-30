use std::collections::{BTreeMap, BTreeSet};
use std::fmt;
use std::future::Future;
use std::io::{self, Cursor};
use std::ops::{Bound, RangeBounds};
use std::pin::Pin;
use std::sync::{Arc, Mutex, MutexGuard};
use std::time::Duration;

use futures_util::{Stream, StreamExt};
use openraft::impls::leader_id_adv::LeaderId;
use openraft::impls::BasicNode;
use openraft::impls::Entry;
use openraft::impls::Vote;
use openraft::storage::Snapshot;
use openraft::storage::SnapshotMeta;
use openraft::storage::{EntryResponder, IOFlushed, LogState, RaftLogStorage, RaftStateMachine};
use openraft::type_config::alias::{
    LogIdOf, SnapshotMetaOf, SnapshotOf, StoredMembershipOf, VoteOf,
};
use openraft::EntryPayload;
use openraft::LogId;
use openraft::OptionalSend;
use openraft::Raft;
use openraft::RaftLogReader;
use openraft::RaftSnapshotBuilder;
use openraft::RaftTypeConfig;
use openraft::ReadPolicy;
use openraft::StoredMembership;
use placement::NodeId;

use crate::control_plane::{
    AuthorityIncarnation, ClusterRuntimeMapSnapshot, ControlPlaneError, NodeAvailabilityState,
    NodeMembershipState,
};
use crate::control_plane_command::{
    ControlPlaneCommand, ControlPlaneCommandResponse, ControlPlaneLogId,
    ControlPlaneSnapshotArtifact, ReplicatedControlPlaneStateMachine,
};
use crate::{ClusterEpoch, PgState};

pub type ControlPlaneRaftNodeId = u64;
pub type ControlPlaneRaftTerm = u64;
pub type ControlPlaneRaftLeaderId = LeaderId<ControlPlaneRaftTerm, ControlPlaneRaftNodeId>;
pub type ControlPlaneRaftEntry =
    Entry<ControlPlaneRaftLeaderId, ControlPlaneCommand, ControlPlaneRaftNodeId, BasicNode>;

#[derive(Debug)]
pub enum ControlPlaneRaftApplyResponse {
    Blank,
    Membership,
    Applied(ControlPlaneCommandResponse),
    Rejected(ControlPlaneError),
}

#[derive(Debug)]
pub enum ControlPlaneRaftCommandOutcome {
    Applied(ControlPlaneCommandResponse),
    Rejected(ControlPlaneError),
}

#[derive(Debug)]
pub struct SubmittedControlPlaneRaftCommand {
    log_id: LogIdOf<ControlPlaneRaftTypeConfig>,
    outcome: ControlPlaneRaftCommandOutcome,
}

impl SubmittedControlPlaneRaftCommand {
    #[must_use]
    pub fn log_id(&self) -> LogIdOf<ControlPlaneRaftTypeConfig> {
        self.log_id
    }

    #[must_use]
    pub fn outcome(&self) -> &ControlPlaneRaftCommandOutcome {
        &self.outcome
    }

    #[must_use]
    pub fn into_outcome(self) -> ControlPlaneRaftCommandOutcome {
        self.outcome
    }
}

pub struct ControlPlaneRaftAuthority {
    raft: Raft<ControlPlaneRaftTypeConfig, ControlPlaneRaftStateMachine>,
    log_store: Option<ControlPlaneRaftLogStore>,
}

pub type ControlPlaneRaftFuture<'a, T> = Pin<Box<dyn Future<Output = T> + Send + 'a>>;

pub trait ControlPlaneRaftLinearizedCommandSink {
    fn submit_control_plane_command(
        &self,
        command: ControlPlaneCommand,
    ) -> ControlPlaneRaftFuture<'_, Result<SubmittedControlPlaneRaftCommand, ControlPlaneError>>;
}

pub trait ControlPlaneRaftLinearizedRuntimeMapSource {
    fn linearized_runtime_map_snapshot(
        &self,
        issued_at_ms: u64,
    ) -> ControlPlaneRaftFuture<'_, Result<ClusterRuntimeMapSnapshot, ControlPlaneError>>;
}

pub trait ControlPlaneRaftLinearizedAuthority:
    ControlPlaneRaftLinearizedCommandSink + ControlPlaneRaftLinearizedRuntimeMapSource
{
}

impl<T> ControlPlaneRaftLinearizedAuthority for T where
    T: ControlPlaneRaftLinearizedCommandSink + ControlPlaneRaftLinearizedRuntimeMapSource
{
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ControlPlaneRaftAuthorityStatus {
    node_id: ControlPlaneRaftNodeId,
    current_leader: Option<ControlPlaneRaftNodeId>,
    persisted_vote: Option<VoteOf<ControlPlaneRaftTypeConfig>>,
    current_term: Option<ControlPlaneRaftTerm>,
    last_log_id: Option<LogIdOf<ControlPlaneRaftTypeConfig>>,
    last_purged_log_id: Option<LogIdOf<ControlPlaneRaftTypeConfig>>,
    committed: Option<LogIdOf<ControlPlaneRaftTypeConfig>>,
    applied: Option<LogIdOf<ControlPlaneRaftTypeConfig>>,
    current_snapshot: Option<LogIdOf<ControlPlaneRaftTypeConfig>>,
    authority_incarnation: AuthorityIncarnation,
    current_cluster_epoch: ClusterEpoch,
    retained_history_count: usize,
    oldest_retained_history_epoch: Option<ClusterEpoch>,
    newest_retained_history_epoch: Option<ClusterEpoch>,
    oldest_storage_history_floor_epoch: Option<ClusterEpoch>,
    storage_node_count: usize,
    joining_storage_node_count: usize,
    active_storage_node_count: usize,
    draining_storage_node_count: usize,
    out_storage_node_count: usize,
    removed_storage_node_count: usize,
    healthy_storage_node_count: usize,
    suspect_storage_node_count: usize,
    unavailable_storage_node_count: usize,
    pg_count: usize,
    active_pg_count: usize,
    peering_pg_count: usize,
    degraded_pg_count: usize,
    backfilling_pg_count: usize,
    inconsistent_pg_count: usize,
    active_primary_pg_count: usize,
    peering_metadata_transfer_pg_count: usize,
    metadata_transfer_fenced_pg_count: usize,
    effective_membership_log_id: Option<LogIdOf<ControlPlaneRaftTypeConfig>>,
    effective_voters: BTreeSet<ControlPlaneRaftNodeId>,
    effective_learners: BTreeSet<ControlPlaneRaftNodeId>,
    applied_membership_log_id: Option<LogIdOf<ControlPlaneRaftTypeConfig>>,
    applied_voters: BTreeSet<ControlPlaneRaftNodeId>,
    applied_learners: BTreeSet<ControlPlaneRaftNodeId>,
}

impl ControlPlaneRaftAuthorityStatus {
    #[must_use]
    pub fn node_id(&self) -> ControlPlaneRaftNodeId {
        self.node_id
    }

    #[must_use]
    pub fn current_leader(&self) -> Option<ControlPlaneRaftNodeId> {
        self.current_leader
    }

    #[must_use]
    pub fn persisted_vote(&self) -> Option<VoteOf<ControlPlaneRaftTypeConfig>> {
        self.persisted_vote
    }

    #[must_use]
    pub fn current_term(&self) -> Option<ControlPlaneRaftTerm> {
        self.current_term
    }

    #[must_use]
    pub fn last_log_id(&self) -> Option<LogIdOf<ControlPlaneRaftTypeConfig>> {
        self.last_log_id
    }

    #[must_use]
    pub fn last_purged_log_id(&self) -> Option<LogIdOf<ControlPlaneRaftTypeConfig>> {
        self.last_purged_log_id
    }

    #[must_use]
    pub fn committed(&self) -> Option<LogIdOf<ControlPlaneRaftTypeConfig>> {
        self.committed
    }

    #[must_use]
    pub fn applied(&self) -> Option<LogIdOf<ControlPlaneRaftTypeConfig>> {
        self.applied
    }

    #[must_use]
    pub fn current_snapshot(&self) -> Option<LogIdOf<ControlPlaneRaftTypeConfig>> {
        self.current_snapshot
    }

    #[must_use]
    pub fn authority_incarnation(&self) -> AuthorityIncarnation {
        self.authority_incarnation
    }

    #[must_use]
    pub fn current_cluster_epoch(&self) -> ClusterEpoch {
        self.current_cluster_epoch
    }

    #[must_use]
    pub fn retained_history_count(&self) -> usize {
        self.retained_history_count
    }

    #[must_use]
    pub fn oldest_retained_history_epoch(&self) -> Option<ClusterEpoch> {
        self.oldest_retained_history_epoch
    }

    #[must_use]
    pub fn newest_retained_history_epoch(&self) -> Option<ClusterEpoch> {
        self.newest_retained_history_epoch
    }

    #[must_use]
    pub fn oldest_storage_history_floor_epoch(&self) -> Option<ClusterEpoch> {
        self.oldest_storage_history_floor_epoch
    }

    #[must_use]
    pub fn storage_node_count(&self) -> usize {
        self.storage_node_count
    }

    #[must_use]
    pub fn joining_storage_node_count(&self) -> usize {
        self.joining_storage_node_count
    }

    #[must_use]
    pub fn active_storage_node_count(&self) -> usize {
        self.active_storage_node_count
    }

    #[must_use]
    pub fn draining_storage_node_count(&self) -> usize {
        self.draining_storage_node_count
    }

    #[must_use]
    pub fn out_storage_node_count(&self) -> usize {
        self.out_storage_node_count
    }

    #[must_use]
    pub fn removed_storage_node_count(&self) -> usize {
        self.removed_storage_node_count
    }

    #[must_use]
    pub fn healthy_storage_node_count(&self) -> usize {
        self.healthy_storage_node_count
    }

    #[must_use]
    pub fn suspect_storage_node_count(&self) -> usize {
        self.suspect_storage_node_count
    }

    #[must_use]
    pub fn unavailable_storage_node_count(&self) -> usize {
        self.unavailable_storage_node_count
    }

    #[must_use]
    pub fn pg_count(&self) -> usize {
        self.pg_count
    }

    #[must_use]
    pub fn active_pg_count(&self) -> usize {
        self.active_pg_count
    }

    #[must_use]
    pub fn peering_pg_count(&self) -> usize {
        self.peering_pg_count
    }

    #[must_use]
    pub fn degraded_pg_count(&self) -> usize {
        self.degraded_pg_count
    }

    #[must_use]
    pub fn backfilling_pg_count(&self) -> usize {
        self.backfilling_pg_count
    }

    #[must_use]
    pub fn inconsistent_pg_count(&self) -> usize {
        self.inconsistent_pg_count
    }

    #[must_use]
    pub fn active_primary_pg_count(&self) -> usize {
        self.active_primary_pg_count
    }

    #[must_use]
    pub fn peering_metadata_transfer_pg_count(&self) -> usize {
        self.peering_metadata_transfer_pg_count
    }

    #[must_use]
    pub fn metadata_transfer_fenced_pg_count(&self) -> usize {
        self.metadata_transfer_fenced_pg_count
    }

    #[must_use]
    pub fn effective_membership_log_id(&self) -> Option<LogIdOf<ControlPlaneRaftTypeConfig>> {
        self.effective_membership_log_id
    }

    #[must_use]
    pub fn effective_voters(&self) -> &BTreeSet<ControlPlaneRaftNodeId> {
        &self.effective_voters
    }

    #[must_use]
    pub fn effective_learners(&self) -> &BTreeSet<ControlPlaneRaftNodeId> {
        &self.effective_learners
    }

    #[must_use]
    pub fn applied_membership_log_id(&self) -> Option<LogIdOf<ControlPlaneRaftTypeConfig>> {
        self.applied_membership_log_id
    }

    #[must_use]
    pub fn applied_voters(&self) -> &BTreeSet<ControlPlaneRaftNodeId> {
        &self.applied_voters
    }

    #[must_use]
    pub fn applied_learners(&self) -> &BTreeSet<ControlPlaneRaftNodeId> {
        &self.applied_learners
    }
}

impl ControlPlaneRaftAuthority {
    #[must_use]
    pub fn new(raft: Raft<ControlPlaneRaftTypeConfig, ControlPlaneRaftStateMachine>) -> Self {
        Self {
            raft,
            log_store: None,
        }
    }

    #[must_use]
    pub fn new_with_log_store(
        raft: Raft<ControlPlaneRaftTypeConfig, ControlPlaneRaftStateMachine>,
        log_store: ControlPlaneRaftLogStore,
    ) -> Self {
        Self {
            raft,
            log_store: Some(log_store),
        }
    }

    #[must_use]
    pub fn raft(&self) -> &Raft<ControlPlaneRaftTypeConfig, ControlPlaneRaftStateMachine> {
        &self.raft
    }

    pub async fn initialize_membership(
        &self,
        nodes: BTreeMap<ControlPlaneRaftNodeId, BasicNode>,
    ) -> Result<(), ControlPlaneError> {
        self.raft
            .initialize(nodes)
            .await
            .map_err(|error| openraft_remote_error("initialize", error))?;
        Ok(())
    }

    pub async fn is_initialized(&self) -> Result<bool, ControlPlaneError> {
        self.raft
            .is_initialized()
            .await
            .map_err(|error| openraft_remote_error("is-initialized", error))
    }

    pub async fn replace_voters(
        &self,
        voters: BTreeSet<ControlPlaneRaftNodeId>,
        retain_removed_voters_as_learners: bool,
    ) -> Result<LogIdOf<ControlPlaneRaftTypeConfig>, ControlPlaneError> {
        let response = self
            .raft
            .change_membership(voters, retain_removed_voters_as_learners)
            .await
            .map_err(|error| openraft_remote_error("change-membership", error))?;
        Ok(response.log_id)
    }

    pub async fn add_learner(
        &self,
        node_id: ControlPlaneRaftNodeId,
        node: BasicNode,
        wait_for_catch_up: bool,
    ) -> Result<LogIdOf<ControlPlaneRaftTypeConfig>, ControlPlaneError> {
        let response = self
            .raft
            .add_learner(node_id, node, wait_for_catch_up)
            .await
            .map_err(|error| openraft_remote_error("add-learner", error))?;
        Ok(response.log_id)
    }

    pub async fn transfer_leadership_to(
        &self,
        node_id: ControlPlaneRaftNodeId,
    ) -> Result<(), ControlPlaneError> {
        self.raft
            .trigger()
            .transfer_leader(node_id)
            .await
            .map_err(|error| openraft_remote_error("transfer-leader", error))
    }

    pub async fn wait_for_applied_index_at_least(
        &self,
        index: u64,
        timeout: Duration,
        message: &'static str,
    ) -> Result<(), ControlPlaneError> {
        self.raft
            .wait(Some(timeout))
            .applied_index_at_least(Some(index), message)
            .await
            .map(|_| ())
            .map_err(|error| openraft_remote_error("wait-applied-index", error))
    }

    pub async fn wait_for_current_leader(
        &self,
        leader_id: ControlPlaneRaftNodeId,
        timeout: Duration,
        message: &'static str,
    ) -> Result<(), ControlPlaneError> {
        self.raft
            .wait(Some(timeout))
            .current_leader(leader_id, message)
            .await
            .map(|_| ())
            .map_err(|error| openraft_remote_error("wait-current-leader", error))
    }

    pub async fn submit_control_plane_command(
        &self,
        command: ControlPlaneCommand,
    ) -> Result<SubmittedControlPlaneRaftCommand, ControlPlaneError> {
        submit_control_plane_command_via_openraft(&self.raft, command).await
    }

    pub async fn linearized_runtime_map_snapshot(
        &self,
        issued_at_ms: u64,
    ) -> Result<ClusterRuntimeMapSnapshot, ControlPlaneError> {
        runtime_map_via_openraft_read_index(&self.raft, issued_at_ms).await
    }

    pub async fn status(&self) -> Result<ControlPlaneRaftAuthorityStatus, ControlPlaneError> {
        let node_id = *self.raft.node_id();
        let current_leader = self.raft.current_leader().await;
        let persisted_vote = self
            .log_store
            .as_ref()
            .map(ControlPlaneRaftLogStore::persisted_vote)
            .transpose()
            .map_err(|error| openraft_remote_error("status log-store vote read", error))?
            .flatten();
        let current_term = persisted_vote.map(|vote| vote.leader_id.term);
        let last_purged_log_id = self
            .log_store
            .as_ref()
            .map(ControlPlaneRaftLogStore::last_purged_log_id)
            .transpose()
            .map_err(|error| openraft_remote_error("status log-store read", error))?
            .flatten();
        let (
            last_log_id,
            committed,
            effective_membership_log_id,
            effective_voters,
            effective_learners,
        ) = self
            .raft
            .with_raft_state(|state| {
                let effective_membership = state.membership_state.effective();
                (
                    state.log_ids.last().cloned(),
                    state.local_committed().cloned(),
                    *effective_membership.log_id(),
                    effective_membership.membership().voter_ids().collect(),
                    effective_membership.membership().learner_ids().collect(),
                )
            })
            .await
            .map_err(|error| openraft_remote_error("status raft-state read", error))?;
        let (
            applied,
            current_snapshot,
            authority_incarnation,
            current_cluster_epoch,
            retained_history_count,
            oldest_retained_history_epoch,
            newest_retained_history_epoch,
            oldest_storage_history_floor_epoch,
            storage_node_count,
            joining_storage_node_count,
            active_storage_node_count,
            draining_storage_node_count,
            out_storage_node_count,
            removed_storage_node_count,
            healthy_storage_node_count,
            suspect_storage_node_count,
            unavailable_storage_node_count,
            pg_count,
            active_pg_count,
            peering_pg_count,
            degraded_pg_count,
            backfilling_pg_count,
            inconsistent_pg_count,
            active_primary_pg_count,
            peering_metadata_transfer_pg_count,
            metadata_transfer_fenced_pg_count,
            applied_membership_log_id,
            applied_voters,
            applied_learners,
        ) = self
            .raft
            .with_state_machine(|state_machine| {
                let last_applied = state_machine.last_applied();
                let current_snapshot = state_machine
                    .current_snapshot()
                    .and_then(|snapshot| snapshot.meta.last_log_id);
                let snapshot = state_machine.inner().snapshot();
                let authority_incarnation = snapshot.authority_incarnation();
                let current_cluster_epoch = snapshot.cluster_epoch();
                let retained_history_count = snapshot.cluster_map_history().len();
                let oldest_retained_history_epoch = snapshot
                    .cluster_map_history()
                    .first()
                    .map(|record| record.cluster_epoch());
                let newest_retained_history_epoch = snapshot
                    .cluster_map_history()
                    .last()
                    .map(|record| record.cluster_epoch());
                let oldest_storage_history_floor_epoch = snapshot
                    .nodes()
                    .filter_map(|node| node.cluster_map_history_floor_epoch())
                    .min();
                let mut storage_node_count = 0;
                let mut joining_storage_node_count = 0;
                let mut active_storage_node_count = 0;
                let mut draining_storage_node_count = 0;
                let mut out_storage_node_count = 0;
                let mut removed_storage_node_count = 0;
                let mut healthy_storage_node_count = 0;
                let mut suspect_storage_node_count = 0;
                let mut unavailable_storage_node_count = 0;
                for node in snapshot.nodes() {
                    storage_node_count += 1;
                    match node.membership() {
                        NodeMembershipState::Joining => joining_storage_node_count += 1,
                        NodeMembershipState::Active => active_storage_node_count += 1,
                        NodeMembershipState::Draining => draining_storage_node_count += 1,
                        NodeMembershipState::Out => out_storage_node_count += 1,
                        NodeMembershipState::Removed => removed_storage_node_count += 1,
                    }
                    match node.availability() {
                        NodeAvailabilityState::Healthy => healthy_storage_node_count += 1,
                        NodeAvailabilityState::Suspect => suspect_storage_node_count += 1,
                        NodeAvailabilityState::Unavailable => unavailable_storage_node_count += 1,
                    }
                }
                let mut pg_count = 0;
                let mut active_pg_count = 0;
                let mut peering_pg_count = 0;
                let mut degraded_pg_count = 0;
                let mut backfilling_pg_count = 0;
                let mut inconsistent_pg_count = 0;
                let mut active_primary_pg_count = 0;
                let mut peering_metadata_transfer_pg_count = 0;
                let mut metadata_transfer_fenced_pg_count = 0;
                for pg in snapshot.pgs() {
                    pg_count += 1;
                    match pg.state() {
                        PgState::Active => active_pg_count += 1,
                        PgState::Peering => peering_pg_count += 1,
                        PgState::Degraded => degraded_pg_count += 1,
                        PgState::Backfilling => backfilling_pg_count += 1,
                        PgState::Inconsistent => inconsistent_pg_count += 1,
                    }
                    if pg.active_primary().is_some() {
                        active_primary_pg_count += 1;
                    }
                    if pg.peering_metadata_transfer().is_some() {
                        peering_metadata_transfer_pg_count += 1;
                    }
                    if pg.metadata_transfer_fenced() {
                        metadata_transfer_fenced_pg_count += 1;
                    }
                }
                let membership = state_machine.last_membership();
                let membership_log_id = *membership.log_id();
                let voters = membership.membership().voter_ids().collect();
                let learners = membership.membership().learner_ids().collect();
                Box::pin(async move {
                    (
                        last_applied,
                        current_snapshot,
                        authority_incarnation,
                        current_cluster_epoch,
                        retained_history_count,
                        oldest_retained_history_epoch,
                        newest_retained_history_epoch,
                        oldest_storage_history_floor_epoch,
                        storage_node_count,
                        joining_storage_node_count,
                        active_storage_node_count,
                        draining_storage_node_count,
                        out_storage_node_count,
                        removed_storage_node_count,
                        healthy_storage_node_count,
                        suspect_storage_node_count,
                        unavailable_storage_node_count,
                        pg_count,
                        active_pg_count,
                        peering_pg_count,
                        degraded_pg_count,
                        backfilling_pg_count,
                        inconsistent_pg_count,
                        active_primary_pg_count,
                        peering_metadata_transfer_pg_count,
                        metadata_transfer_fenced_pg_count,
                        membership_log_id,
                        voters,
                        learners,
                    )
                })
            })
            .await
            .map_err(|error| openraft_remote_error("status state-machine read", error))?;
        Ok(ControlPlaneRaftAuthorityStatus {
            node_id,
            current_leader,
            persisted_vote,
            current_term,
            last_log_id,
            last_purged_log_id,
            committed,
            applied,
            current_snapshot,
            authority_incarnation,
            current_cluster_epoch,
            retained_history_count,
            oldest_retained_history_epoch,
            newest_retained_history_epoch,
            oldest_storage_history_floor_epoch,
            storage_node_count,
            joining_storage_node_count,
            active_storage_node_count,
            draining_storage_node_count,
            out_storage_node_count,
            removed_storage_node_count,
            healthy_storage_node_count,
            suspect_storage_node_count,
            unavailable_storage_node_count,
            pg_count,
            active_pg_count,
            peering_pg_count,
            degraded_pg_count,
            backfilling_pg_count,
            inconsistent_pg_count,
            active_primary_pg_count,
            peering_metadata_transfer_pg_count,
            metadata_transfer_fenced_pg_count,
            effective_membership_log_id,
            effective_voters,
            effective_learners,
            applied_membership_log_id,
            applied_voters,
            applied_learners,
        })
    }

    pub async fn shutdown(&self) -> Result<(), ControlPlaneError> {
        self.raft
            .shutdown()
            .await
            .map_err(|error| openraft_remote_error("shutdown", error))
    }
}

impl ControlPlaneRaftLinearizedCommandSink for ControlPlaneRaftAuthority {
    fn submit_control_plane_command(
        &self,
        command: ControlPlaneCommand,
    ) -> ControlPlaneRaftFuture<'_, Result<SubmittedControlPlaneRaftCommand, ControlPlaneError>>
    {
        Box::pin(async move {
            ControlPlaneRaftAuthority::submit_control_plane_command(self, command).await
        })
    }
}

impl ControlPlaneRaftLinearizedRuntimeMapSource for ControlPlaneRaftAuthority {
    fn linearized_runtime_map_snapshot(
        &self,
        issued_at_ms: u64,
    ) -> ControlPlaneRaftFuture<'_, Result<ClusterRuntimeMapSnapshot, ControlPlaneError>> {
        Box::pin(async move {
            ControlPlaneRaftAuthority::linearized_runtime_map_snapshot(self, issued_at_ms).await
        })
    }
}

openraft::declare_raft_types!(
    pub ControlPlaneRaftTypeConfig:
        D = ControlPlaneCommand,
        R = ControlPlaneRaftApplyResponse,
        NodeId = ControlPlaneRaftNodeId,
        Node = BasicNode,
        Term = ControlPlaneRaftTerm,
        LeaderId = ControlPlaneRaftLeaderId,
        Vote = Vote<ControlPlaneRaftLeaderId>,
        Entry = ControlPlaneRaftEntry,
        SnapshotData = Cursor<Vec<u8>>,
);

#[must_use]
pub fn raft_node_id_from_storage_node_id(node_id: NodeId) -> ControlPlaneRaftNodeId {
    u64::from(node_id.as_u32())
}

#[must_use]
pub fn storage_node_id_from_raft_node_id(node_id: ControlPlaneRaftNodeId) -> Option<NodeId> {
    let node_id = u32::try_from(node_id).ok()?;
    Some(NodeId::new(node_id))
}

#[must_use]
pub fn control_plane_log_id_from_raft(
    log_id: LogIdOf<ControlPlaneRaftTypeConfig>,
) -> Option<ControlPlaneLogId> {
    ControlPlaneLogId::new(log_id.committed_leader_id().term, log_id.index())
}

#[must_use]
pub fn raft_log_id_from_control_plane(
    leader_node_id: ControlPlaneRaftNodeId,
    log_id: ControlPlaneLogId,
) -> LogIdOf<ControlPlaneRaftTypeConfig> {
    LogId::new(
        LeaderId {
            term: log_id.term(),
            node_id: leader_node_id,
        },
        log_id.index(),
    )
}

pub fn assert_openraft_type_config() {
    fn assert_config<C: RaftTypeConfig>() {}
    assert_config::<ControlPlaneRaftTypeConfig>();
}

pub async fn submit_control_plane_command_via_openraft(
    raft: &Raft<ControlPlaneRaftTypeConfig, ControlPlaneRaftStateMachine>,
    command: ControlPlaneCommand,
) -> Result<SubmittedControlPlaneRaftCommand, ControlPlaneError> {
    let response = raft
        .client_write(command)
        .await
        .map_err(|error| openraft_remote_error("client-write", error))?;
    let outcome = match response.data {
        ControlPlaneRaftApplyResponse::Applied(response) => {
            ControlPlaneRaftCommandOutcome::Applied(response)
        }
        ControlPlaneRaftApplyResponse::Rejected(error) => {
            ControlPlaneRaftCommandOutcome::Rejected(error)
        }
        ControlPlaneRaftApplyResponse::Blank | ControlPlaneRaftApplyResponse::Membership => {
            return Err(ControlPlaneError::CommandDecode {
                message: format!(
                    "OpenRaft client-write for control-plane command returned non-command response at {}",
                    response.log_id
                ),
            });
        }
    };
    Ok(SubmittedControlPlaneRaftCommand {
        log_id: response.log_id,
        outcome,
    })
}

pub async fn runtime_map_via_openraft_read_index(
    raft: &Raft<ControlPlaneRaftTypeConfig, ControlPlaneRaftStateMachine>,
    issued_at_ms: u64,
) -> Result<ClusterRuntimeMapSnapshot, ControlPlaneError> {
    let read_log_id = raft
        .ensure_linearizable(ReadPolicy::ReadIndex)
        .await
        .map_err(|error| openraft_remote_error("read-index", error))?
        .ok_or_else(|| ControlPlaneError::CommandDecode {
            message: "OpenRaft read-index returned no applied log id".to_string(),
        })?;
    let read_index = control_plane_log_id_from_raft(read_log_id).ok_or_else(|| {
        ControlPlaneError::CommandDecode {
            message: format!("invalid OpenRaft read-index log id for runtime map: {read_log_id}"),
        }
    })?;

    raft.with_state_machine(move |state_machine| {
        Box::pin(async move {
            let Some(last_applied) = state_machine.last_applied() else {
                return Err(ControlPlaneError::ControlPlaneReadIndexNotApplied {
                    read_index,
                    last_applied: state_machine.inner().last_applied(),
                });
            };
            if last_applied.index() < read_log_id.index() {
                return Err(ControlPlaneError::ControlPlaneReadIndexNotApplied {
                    read_index,
                    last_applied: state_machine.inner().last_applied(),
                });
            }
            state_machine.runtime_map_for_current_applied_read_index(issued_at_ms)
        })
    })
    .await
    .map_err(|error| openraft_remote_error("state-machine read", error))?
}

fn control_plane_error_to_io_error(context: &'static str, error: ControlPlaneError) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, format!("{context}: {error}"))
}

fn openraft_remote_error(context: &'static str, error: impl fmt::Display) -> ControlPlaneError {
    ControlPlaneError::RpcRemote {
        message: format!("OpenRaft {context} failed: {error}"),
    }
}

fn raft_log_store_error(message: impl Into<String>) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, message.into())
}

#[derive(Debug, Clone, Default)]
pub struct ControlPlaneRaftLogStore {
    inner: Arc<Mutex<ControlPlaneRaftLogStoreInner>>,
}

#[derive(Debug, Clone, Default)]
pub struct ControlPlaneRaftLogStoreRestartArtifact {
    vote: Option<VoteOf<ControlPlaneRaftTypeConfig>>,
    committed: Option<LogIdOf<ControlPlaneRaftTypeConfig>>,
    last_purged_log_id: Option<LogIdOf<ControlPlaneRaftTypeConfig>>,
    entries: Vec<ControlPlaneRaftEntry>,
}

#[derive(Debug, Clone)]
pub struct ControlPlaneRaftRestartArtifact {
    log_store: ControlPlaneRaftLogStoreRestartArtifact,
    state_machine: ControlPlaneRaftStateMachineRestartArtifact,
}

#[derive(Debug, Default)]
struct ControlPlaneRaftLogStoreInner {
    vote: Option<VoteOf<ControlPlaneRaftTypeConfig>>,
    committed: Option<LogIdOf<ControlPlaneRaftTypeConfig>>,
    last_purged_log_id: Option<LogIdOf<ControlPlaneRaftTypeConfig>>,
    entries: BTreeMap<u64, ControlPlaneRaftEntry>,
}

impl ControlPlaneRaftLogStore {
    #[must_use]
    pub fn empty() -> Self {
        Self::default()
    }

    pub fn export_restart_artifact(
        &self,
    ) -> Result<ControlPlaneRaftLogStoreRestartArtifact, io::Error> {
        let inner = self.lock()?;
        Ok(ControlPlaneRaftLogStoreRestartArtifact {
            vote: inner.vote,
            committed: inner.committed,
            last_purged_log_id: inner.last_purged_log_id,
            entries: inner.entries.values().cloned().collect(),
        })
    }

    pub fn last_purged_log_id(
        &self,
    ) -> Result<Option<LogIdOf<ControlPlaneRaftTypeConfig>>, io::Error> {
        Ok(self.lock()?.last_purged_log_id)
    }

    pub fn persisted_vote(&self) -> Result<Option<VoteOf<ControlPlaneRaftTypeConfig>>, io::Error> {
        Ok(self.lock()?.vote)
    }

    pub fn from_restart_artifact(
        artifact: ControlPlaneRaftLogStoreRestartArtifact,
    ) -> Result<Self, io::Error> {
        let mut inner = ControlPlaneRaftLogStoreInner {
            vote: artifact.vote,
            committed: None,
            last_purged_log_id: artifact.last_purged_log_id,
            entries: BTreeMap::new(),
        };
        Self::validate_contiguous_append(&inner, &artifact.entries)?;
        for entry in artifact.entries {
            inner.entries.insert(entry.log_id.index(), entry);
        }
        Self::validate_committed_update(&inner, artifact.committed)?;
        inner.committed = artifact.committed;
        Self::validate_purged_boundary_has_committed(&inner)?;
        Ok(Self {
            inner: Arc::new(Mutex::new(inner)),
        })
    }

    fn lock(&self) -> Result<MutexGuard<'_, ControlPlaneRaftLogStoreInner>, io::Error> {
        self.inner
            .lock()
            .map_err(|_| io::Error::other("control-plane OpenRaft log store lock poisoned"))
    }

    fn validate_contiguous_append(
        inner: &ControlPlaneRaftLogStoreInner,
        entries: &[ControlPlaneRaftEntry],
    ) -> Result<(), io::Error> {
        let Some(first) = entries.first() else {
            return Ok(());
        };
        let current_last_log_id = inner.last_log_id();
        let expected_first_index = match current_last_log_id {
            Some(log_id) => log_id.index().checked_add(1).ok_or_else(|| {
                raft_log_store_error("cannot append after u64::MAX OpenRaft log index")
            })?,
            None => 0,
        };
        if first.log_id.index() != expected_first_index {
            return Err(raft_log_store_error(format!(
                "control-plane OpenRaft append starts at index {}, expected {}",
                first.log_id.index(),
                expected_first_index
            )));
        }

        let mut expected_index = expected_first_index;
        for entry in entries {
            if entry.log_id.index() != expected_index {
                return Err(raft_log_store_error(format!(
                    "control-plane OpenRaft append leaves a log hole at index {expected_index}; next entry is {}",
                    entry.log_id.index()
                )));
            }
            expected_index = expected_index.checked_add(1).ok_or_else(|| {
                raft_log_store_error("control-plane OpenRaft append range overflows u64")
            })?;
        }
        Ok(())
    }

    fn range_start<RB>(range: &RB) -> Result<Option<u64>, io::Error>
    where
        RB: RangeBounds<u64>,
    {
        match range.start_bound() {
            Bound::Included(start) => Ok(Some(*start)),
            Bound::Excluded(start) => Ok(start.checked_add(1)),
            Bound::Unbounded => Ok(Some(0)),
        }
    }

    fn range_end_exclusive<RB>(range: &RB) -> Option<u64>
    where
        RB: RangeBounds<u64>,
    {
        match range.end_bound() {
            Bound::Included(end) => end.checked_add(1),
            Bound::Excluded(end) => Some(*end),
            Bound::Unbounded => None,
        }
    }

    fn before_range_end(index: u64, end_exclusive: Option<u64>) -> bool {
        end_exclusive.is_none_or(|end_exclusive| index < end_exclusive)
    }

    fn validate_known_log_id(
        inner: &ControlPlaneRaftLogStoreInner,
        context: &'static str,
        log_id: LogIdOf<ControlPlaneRaftTypeConfig>,
    ) -> Result<(), io::Error> {
        let Some(current_last_log_id) = inner.last_log_id() else {
            return Err(raft_log_store_error(format!(
                "cannot {context} {log_id}; control-plane OpenRaft log is empty"
            )));
        };
        if log_id.index() > current_last_log_id.index() {
            return Err(raft_log_store_error(format!(
                "cannot {context} {log_id}; current last log id is {current_last_log_id}"
            )));
        }
        if let Some(last_purged_log_id) = inner.last_purged_log_id {
            if log_id.index() < last_purged_log_id.index() {
                return Err(raft_log_store_error(format!(
                    "cannot {context} {log_id}; it is before purged boundary {last_purged_log_id}"
                )));
            }
            if log_id.index() == last_purged_log_id.index() {
                if log_id == last_purged_log_id {
                    return Ok(());
                }
                return Err(raft_log_store_error(format!(
                    "cannot {context} mismatched purged log id {log_id}; purged boundary is {last_purged_log_id}"
                )));
            }
        }
        let Some(entry) = inner.entries.get(&log_id.index()) else {
            return Err(raft_log_store_error(format!(
                "cannot {context} {log_id}; control-plane OpenRaft log has no entry at that index"
            )));
        };
        if entry.log_id != log_id {
            return Err(raft_log_store_error(format!(
                "cannot {context} mismatched log id {log_id}; stored {}",
                entry.log_id
            )));
        }
        Ok(())
    }

    fn validate_committed_update(
        inner: &ControlPlaneRaftLogStoreInner,
        committed: Option<LogIdOf<ControlPlaneRaftTypeConfig>>,
    ) -> Result<(), io::Error> {
        let Some(committed) = committed else {
            if inner.committed.is_some() {
                return Err(raft_log_store_error(
                    "cannot clear control-plane OpenRaft committed log id",
                ));
            }
            return Ok(());
        };
        if let Some(previous_committed) = inner.committed {
            if committed.index() < previous_committed.index() {
                return Err(raft_log_store_error(format!(
                    "cannot regress control-plane OpenRaft committed log id from {previous_committed} to {committed}"
                )));
            }
            if committed.index() == previous_committed.index() && committed != previous_committed {
                return Err(raft_log_store_error(format!(
                    "cannot change control-plane OpenRaft committed log id at index {} from {previous_committed} to {committed}",
                    committed.index()
                )));
            }
        }
        Self::validate_known_log_id(inner, "commit", committed)?;
        Self::validate_vote_covers_committed(inner.vote, committed)
    }

    fn validate_vote_update(
        inner: &ControlPlaneRaftLogStoreInner,
        vote: VoteOf<ControlPlaneRaftTypeConfig>,
    ) -> Result<(), io::Error> {
        let Some(previous_vote) = inner.vote else {
            return Ok(());
        };
        if matches!(
            vote.partial_cmp(&previous_vote),
            Some(std::cmp::Ordering::Equal | std::cmp::Ordering::Greater)
        ) {
            return Ok(());
        }
        Err(raft_log_store_error(format!(
            "cannot regress control-plane OpenRaft vote from {previous_vote} to {vote}"
        )))
    }

    fn validate_vote_covers_committed(
        vote: Option<VoteOf<ControlPlaneRaftTypeConfig>>,
        committed: LogIdOf<ControlPlaneRaftTypeConfig>,
    ) -> Result<(), io::Error> {
        let Some(vote) = vote else {
            return Err(raft_log_store_error(format!(
                "control-plane OpenRaft log store is missing vote state for committed log id {committed}"
            )));
        };
        if vote.leader_id >= *committed.committed_leader_id() {
            return Ok(());
        }
        Err(raft_log_store_error(format!(
            "control-plane OpenRaft log store vote {vote} does not cover committed log id {committed}"
        )))
    }

    fn validate_purged_boundary_has_committed(
        inner: &ControlPlaneRaftLogStoreInner,
    ) -> Result<(), io::Error> {
        let Some(last_purged_log_id) = inner.last_purged_log_id else {
            return Ok(());
        };
        let Some(committed) = inner.committed else {
            return Err(raft_log_store_error(format!(
                "control-plane OpenRaft log store has purged boundary {last_purged_log_id} without a committed restart gate"
            )));
        };
        if last_purged_log_id.index() <= committed.index() {
            return Ok(());
        }
        Err(raft_log_store_error(format!(
            "control-plane OpenRaft purged boundary {last_purged_log_id} is after committed log id {committed}"
        )))
    }
}

impl ControlPlaneRaftLogStoreInner {
    fn last_log_id(&self) -> Option<LogIdOf<ControlPlaneRaftTypeConfig>> {
        self.entries
            .last_key_value()
            .map(|(_, entry)| entry.log_id)
            .or(self.last_purged_log_id)
    }
}

impl ControlPlaneRaftRestartArtifact {
    pub fn capture(
        log_store: &ControlPlaneRaftLogStore,
        state_machine: &ControlPlaneRaftStateMachine,
    ) -> Result<Self, io::Error> {
        Ok(Self {
            log_store: log_store.export_restart_artifact()?,
            state_machine: state_machine.export_restart_artifact(),
        })
    }

    pub fn restore(
        self,
    ) -> Result<(ControlPlaneRaftLogStore, ControlPlaneRaftStateMachine), io::Error> {
        let log_store = ControlPlaneRaftLogStore::from_restart_artifact(self.log_store.clone())?;
        let state_machine =
            ControlPlaneRaftStateMachine::from_restart_artifact(self.state_machine.clone())
                .map_err(|error| {
                    control_plane_error_to_io_error("OpenRaft state-machine restart", error)
                })?;
        Self::validate_log_store_state_machine_pair(&self.log_store, &self.state_machine)?;
        Ok((log_store, state_machine))
    }

    fn validate_log_store_state_machine_pair(
        log_store: &ControlPlaneRaftLogStoreRestartArtifact,
        state_machine: &ControlPlaneRaftStateMachineRestartArtifact,
    ) -> Result<(), io::Error> {
        if let Some(last_purged_log_id) = log_store.last_purged_log_id {
            match state_machine.last_applied {
                Some(last_applied) if last_applied.index() > last_purged_log_id.index() => {}
                Some(last_applied) if last_applied == last_purged_log_id => {}
                Some(last_applied) => {
                    return Err(raft_log_store_error(format!(
                        "control-plane OpenRaft state-machine applied log id {last_applied} is behind purged boundary {last_purged_log_id}"
                    )));
                }
                None => {
                    return Err(raft_log_store_error(format!(
                        "control-plane OpenRaft state-machine has no applied log id but log is purged through {last_purged_log_id}"
                    )));
                }
            }
        }

        let Some(last_applied) = state_machine.last_applied else {
            return Ok(());
        };
        let Some(known_applied) =
            Self::log_store_artifact_log_id_at(log_store, last_applied.index())
        else {
            return Err(raft_log_store_error(format!(
                "control-plane OpenRaft state-machine applied log id {last_applied} is not retained or purged in the log store"
            )));
        };
        if known_applied != last_applied {
            return Err(raft_log_store_error(format!(
                "control-plane OpenRaft state-machine applied log id {last_applied} does not match log-store log id {known_applied}"
            )));
        }

        if is_openraft_bootstrap_log_id(last_applied) {
            return Ok(());
        }
        let Some(committed) = log_store.committed else {
            return Err(raft_log_store_error(format!(
                "control-plane OpenRaft state-machine applied log id {last_applied} has no committed restart gate"
            )));
        };
        if last_applied.index() > committed.index() {
            return Err(raft_log_store_error(format!(
                "control-plane OpenRaft state-machine applied log id {last_applied} is after committed restart gate {committed}"
            )));
        }
        if last_applied.index() == committed.index() && last_applied != committed {
            return Err(raft_log_store_error(format!(
                "control-plane OpenRaft state-machine applied log id {last_applied} conflicts with committed restart gate {committed}"
            )));
        }
        Ok(())
    }

    fn log_store_artifact_log_id_at(
        log_store: &ControlPlaneRaftLogStoreRestartArtifact,
        index: u64,
    ) -> Option<LogIdOf<ControlPlaneRaftTypeConfig>> {
        if let Some(last_purged_log_id) = log_store.last_purged_log_id {
            if index < last_purged_log_id.index() {
                return None;
            }
            if index == last_purged_log_id.index() {
                return Some(last_purged_log_id);
            }
        }
        log_store
            .entries
            .iter()
            .find(|entry| entry.log_id.index() == index)
            .map(|entry| entry.log_id)
    }
}

impl RaftLogReader<ControlPlaneRaftTypeConfig> for ControlPlaneRaftLogStore {
    async fn try_get_log_entries<RB>(
        &mut self,
        range: RB,
    ) -> Result<Vec<ControlPlaneRaftEntry>, io::Error>
    where
        RB: RangeBounds<u64> + Clone + std::fmt::Debug + OptionalSend,
    {
        let Some(start) = Self::range_start(&range)? else {
            return Ok(Vec::new());
        };
        let end_exclusive = Self::range_end_exclusive(&range);
        if end_exclusive.is_some_and(|end_exclusive| start >= end_exclusive) {
            return Ok(Vec::new());
        }

        let inner = self.lock()?;
        let Some((&first_present, _)) = inner.entries.first_key_value() else {
            return Ok(Vec::new());
        };
        let Some((&last_present, _)) = inner.entries.last_key_value() else {
            return Ok(Vec::new());
        };

        let mut entries = Vec::new();
        let mut index = start.max(first_present);
        while index <= last_present && Self::before_range_end(index, end_exclusive) {
            let entry = inner.entries.get(&index).ok_or_else(|| {
                raft_log_store_error(format!(
                    "control-plane OpenRaft log hole at readable index {index}"
                ))
            })?;
            entries.push(entry.clone());
            let Some(next_index) = index.checked_add(1) else {
                break;
            };
            index = next_index;
        }
        Ok(entries)
    }

    async fn read_vote(&mut self) -> Result<Option<VoteOf<ControlPlaneRaftTypeConfig>>, io::Error> {
        Ok(self.lock()?.vote)
    }
}

impl RaftLogStorage<ControlPlaneRaftTypeConfig> for ControlPlaneRaftLogStore {
    type LogReader = Self;

    async fn get_log_state(&mut self) -> Result<LogState<ControlPlaneRaftTypeConfig>, io::Error> {
        let inner = self.lock()?;
        Ok(LogState {
            last_purged_log_id: inner.last_purged_log_id,
            last_log_id: inner.last_log_id(),
        })
    }

    async fn get_log_reader(&mut self) -> Self::LogReader {
        self.clone()
    }

    async fn save_vote(
        &mut self,
        vote: &VoteOf<ControlPlaneRaftTypeConfig>,
    ) -> Result<(), io::Error> {
        let mut inner = self.lock()?;
        Self::validate_vote_update(&inner, *vote)?;
        inner.vote = Some(*vote);
        Ok(())
    }

    async fn save_committed(
        &mut self,
        committed: Option<LogIdOf<ControlPlaneRaftTypeConfig>>,
    ) -> Result<(), io::Error> {
        let mut inner = self.lock()?;
        Self::validate_committed_update(&inner, committed)?;
        inner.committed = committed;
        Ok(())
    }

    async fn read_committed(
        &mut self,
    ) -> Result<Option<LogIdOf<ControlPlaneRaftTypeConfig>>, io::Error> {
        Ok(self.lock()?.committed)
    }

    async fn append<I>(
        &mut self,
        entries: I,
        callback: IOFlushed<ControlPlaneRaftTypeConfig>,
    ) -> Result<(), io::Error>
    where
        I: IntoIterator<Item = ControlPlaneRaftEntry> + OptionalSend,
        I::IntoIter: OptionalSend,
    {
        let entries = entries.into_iter().collect::<Vec<_>>();
        {
            let mut inner = self.lock()?;
            if let Err(error) = Self::validate_contiguous_append(&inner, &entries) {
                let message = error.to_string();
                callback.io_completed(Err(raft_log_store_error(message.clone())));
                return Err(raft_log_store_error(message));
            }
            for entry in entries {
                inner.entries.insert(entry.log_id.index(), entry);
            }
        }
        callback.io_completed(Ok(()));
        Ok(())
    }

    async fn truncate_after(
        &mut self,
        last_log_id: Option<LogIdOf<ControlPlaneRaftTypeConfig>>,
    ) -> Result<(), io::Error> {
        let mut inner = self.lock()?;
        if let Some(committed) = inner.committed {
            match last_log_id {
                Some(last_log_id) if last_log_id.index() >= committed.index() => {}
                Some(last_log_id) => {
                    return Err(raft_log_store_error(format!(
                        "cannot truncate control-plane OpenRaft log after {last_log_id}; committed log id is {committed}"
                    )));
                }
                None => {
                    return Err(raft_log_store_error(format!(
                        "cannot clear control-plane OpenRaft log; committed log id is {committed}"
                    )));
                }
            }
        }
        let Some(last_log_id) = last_log_id else {
            inner.entries.clear();
            return Ok(());
        };

        Self::validate_known_log_id(&inner, "truncate after", last_log_id)?;
        inner
            .entries
            .retain(|index, _| *index <= last_log_id.index());
        Ok(())
    }

    async fn purge(
        &mut self,
        log_id: LogIdOf<ControlPlaneRaftTypeConfig>,
    ) -> Result<(), io::Error> {
        let mut inner = self.lock()?;
        if let Some(last_purged_log_id) = inner.last_purged_log_id {
            if log_id.index() <= last_purged_log_id.index() {
                if log_id == last_purged_log_id {
                    return Ok(());
                }
                return Err(raft_log_store_error(format!(
                    "cannot repurge control-plane OpenRaft log to {log_id}; current purged boundary is {last_purged_log_id}"
                )));
            }
        }
        if inner.committed.is_none() {
            return Err(raft_log_store_error(format!(
                "cannot purge control-plane OpenRaft log to {log_id}; no committed restart gate"
            )));
        }
        Self::validate_vote_covers_committed(inner.vote, log_id)?;
        inner.entries.retain(|index, _| *index > log_id.index());
        inner.last_purged_log_id = Some(log_id);
        if inner
            .committed
            .is_some_and(|committed| committed.index() < log_id.index())
        {
            inner.committed = Some(log_id);
        }
        Ok(())
    }
}

fn is_openraft_bootstrap_log_id(log_id: LogIdOf<ControlPlaneRaftTypeConfig>) -> bool {
    log_id.index() == 0 && log_id.committed_leader_id().term == 0
}

#[derive(Debug, Clone)]
pub struct ControlPlaneRaftSnapshotBuilder {
    snapshot: Result<SnapshotOf<ControlPlaneRaftTypeConfig>, String>,
}

impl ControlPlaneRaftSnapshotBuilder {
    #[must_use]
    pub fn new(snapshot: SnapshotOf<ControlPlaneRaftTypeConfig>) -> Self {
        Self {
            snapshot: Ok(snapshot),
        }
    }

    #[must_use]
    pub fn from_error(error: ControlPlaneError) -> Self {
        Self {
            snapshot: Err(error.to_string()),
        }
    }
}

impl RaftSnapshotBuilder<ControlPlaneRaftTypeConfig> for ControlPlaneRaftSnapshotBuilder {
    async fn build_snapshot(
        &mut self,
    ) -> Result<SnapshotOf<ControlPlaneRaftTypeConfig>, io::Error> {
        self.snapshot.clone().map_err(|message| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                format!("control-plane OpenRaft snapshot build failed: {message}"),
            )
        })
    }
}

#[derive(Debug, Clone)]
pub struct ControlPlaneRaftStateMachine {
    inner: ReplicatedControlPlaneStateMachine,
    last_applied: Option<LogIdOf<ControlPlaneRaftTypeConfig>>,
    last_membership: StoredMembershipOf<ControlPlaneRaftTypeConfig>,
    current_snapshot: Option<SnapshotOf<ControlPlaneRaftTypeConfig>>,
}

#[derive(Debug, Clone)]
pub struct ControlPlaneRaftStateMachineRestartArtifact {
    inner: ReplicatedControlPlaneStateMachine,
    last_applied: Option<LogIdOf<ControlPlaneRaftTypeConfig>>,
    last_membership: StoredMembershipOf<ControlPlaneRaftTypeConfig>,
}

impl ControlPlaneRaftStateMachine {
    #[must_use]
    pub fn empty() -> Self {
        Self::from_parts_unchecked(
            ReplicatedControlPlaneStateMachine::empty(),
            None,
            StoredMembership::default(),
        )
    }

    pub fn new(
        inner: ReplicatedControlPlaneStateMachine,
        last_applied: Option<LogIdOf<ControlPlaneRaftTypeConfig>>,
        last_membership: StoredMembershipOf<ControlPlaneRaftTypeConfig>,
    ) -> Result<Self, ControlPlaneError> {
        Self::validate_restart_log_id_consistency(&inner, last_applied)?;
        Self::validate_snapshot_membership_position(last_applied, &last_membership)?;
        Ok(Self::from_parts_unchecked(
            inner,
            last_applied,
            last_membership,
        ))
    }

    #[must_use]
    pub fn export_restart_artifact(&self) -> ControlPlaneRaftStateMachineRestartArtifact {
        ControlPlaneRaftStateMachineRestartArtifact {
            inner: self.inner.clone(),
            last_applied: self.last_applied,
            last_membership: self.last_membership.clone(),
        }
    }

    pub fn from_restart_artifact(
        artifact: ControlPlaneRaftStateMachineRestartArtifact,
    ) -> Result<Self, ControlPlaneError> {
        Self::new(
            artifact.inner,
            artifact.last_applied,
            artifact.last_membership,
        )
    }

    fn from_parts_unchecked(
        inner: ReplicatedControlPlaneStateMachine,
        last_applied: Option<LogIdOf<ControlPlaneRaftTypeConfig>>,
        last_membership: StoredMembershipOf<ControlPlaneRaftTypeConfig>,
    ) -> Self {
        Self {
            inner,
            last_applied,
            last_membership,
            current_snapshot: None,
        }
    }

    #[must_use]
    pub fn inner(&self) -> &ReplicatedControlPlaneStateMachine {
        &self.inner
    }

    #[must_use]
    pub fn last_applied(&self) -> Option<LogIdOf<ControlPlaneRaftTypeConfig>> {
        self.last_applied
    }

    #[must_use]
    pub fn last_membership(&self) -> &StoredMembershipOf<ControlPlaneRaftTypeConfig> {
        &self.last_membership
    }

    #[must_use]
    pub fn current_snapshot(&self) -> Option<&SnapshotOf<ControlPlaneRaftTypeConfig>> {
        self.current_snapshot.as_ref()
    }

    #[must_use]
    pub fn applied_state(
        &self,
    ) -> (
        Option<LogIdOf<ControlPlaneRaftTypeConfig>>,
        StoredMembershipOf<ControlPlaneRaftTypeConfig>,
    ) {
        (self.last_applied, self.last_membership.clone())
    }

    pub fn runtime_map_for_applied_read_index(
        &self,
        read_index: LogIdOf<ControlPlaneRaftTypeConfig>,
        issued_at_ms: u64,
    ) -> Result<ClusterRuntimeMapSnapshot, ControlPlaneError> {
        let control_plane_read_index = control_plane_log_id_from_raft(read_index).ok_or_else(|| {
            ControlPlaneError::CommandDecode {
                message: format!(
                    "invalid OpenRaft read-index log id for control-plane runtime map proof: {read_index}"
                ),
            }
        })?;
        if self.last_applied != Some(read_index) {
            if let Some(last_applied) = self.last_applied {
                if last_applied.committed_leader_id().term == read_index.committed_leader_id().term
                    && last_applied.index() == read_index.index()
                {
                    return Err(ControlPlaneError::CommandDecode {
                        message: format!(
                            "OpenRaft read-index log id {read_index} does not match applied log id {last_applied}"
                        ),
                    });
                }
            }
            return Err(ControlPlaneError::ControlPlaneReadIndexNotApplied {
                read_index: control_plane_read_index,
                last_applied: self.inner.last_applied(),
            });
        }
        self.inner
            .runtime_map_for_read_index(control_plane_read_index, issued_at_ms)
    }

    pub fn runtime_map_for_current_applied_read_index(
        &self,
        issued_at_ms: u64,
    ) -> Result<ClusterRuntimeMapSnapshot, ControlPlaneError> {
        let last_applied = self
            .last_applied
            .ok_or_else(|| ControlPlaneError::CommandDecode {
                message: "cannot build OpenRaft read-index runtime map before any log is applied"
                    .to_string(),
            })?;
        self.runtime_map_for_applied_read_index(last_applied, issued_at_ms)
    }

    pub fn apply_entry(
        &mut self,
        entry: ControlPlaneRaftEntry,
    ) -> Result<ControlPlaneRaftApplyResponse, ControlPlaneError> {
        let raft_log_id = entry.log_id;
        self.validate_apply_position(raft_log_id)?;
        match entry.payload {
            EntryPayload::Blank => {
                let control_plane_log_id = Self::control_plane_log_id_for_entry(raft_log_id)?;
                self.inner.apply_committed_noop(control_plane_log_id)?;
                self.last_applied = Some(raft_log_id);
                Ok(ControlPlaneRaftApplyResponse::Blank)
            }
            EntryPayload::Membership(membership) => {
                if !is_openraft_bootstrap_log_id(raft_log_id) {
                    let control_plane_log_id = Self::control_plane_log_id_for_entry(raft_log_id)?;
                    self.inner.apply_committed_noop(control_plane_log_id)?;
                }
                self.last_membership = StoredMembership::new(Some(raft_log_id), membership);
                self.last_applied = Some(raft_log_id);
                Ok(ControlPlaneRaftApplyResponse::Membership)
            }
            EntryPayload::Normal(command) => {
                let control_plane_log_id = Self::control_plane_log_id_for_entry(raft_log_id)?;
                let applied = self
                    .inner
                    .apply_committed_command(control_plane_log_id, command)?;
                self.last_applied = Some(raft_log_id);
                match applied.into_outcome() {
                    crate::control_plane_command::CommittedControlPlaneCommandOutcome::Applied(
                        applied,
                    ) => Ok(ControlPlaneRaftApplyResponse::Applied(
                        applied.response().clone(),
                    )),
                    crate::control_plane_command::CommittedControlPlaneCommandOutcome::Rejected(
                        error,
                    ) => Ok(ControlPlaneRaftApplyResponse::Rejected(error)),
                }
            }
        }
    }

    fn control_plane_log_id_for_entry(
        raft_log_id: LogIdOf<ControlPlaneRaftTypeConfig>,
    ) -> Result<ControlPlaneLogId, ControlPlaneError> {
        control_plane_log_id_from_raft(raft_log_id).ok_or_else(|| {
            ControlPlaneError::CommandDecode {
                message: format!("invalid OpenRaft log id for control-plane entry: {raft_log_id}"),
            }
        })
    }

    fn validate_apply_position(
        &self,
        raft_log_id: LogIdOf<ControlPlaneRaftTypeConfig>,
    ) -> Result<(), ControlPlaneError> {
        let Some(last_applied) = self.last_applied else {
            if is_openraft_bootstrap_log_id(raft_log_id) {
                return Ok(());
            }
            if raft_log_id.index() == 0 {
                return Err(ControlPlaneError::CommandDecode {
                    message: format!(
                        "invalid OpenRaft log id for first control-plane entry: {raft_log_id}"
                    ),
                });
            }
            if raft_log_id.index() != 1 {
                return Err(ControlPlaneError::ControlPlaneLogIndexMismatch {
                    expected_index: 1,
                    actual_index: raft_log_id.index(),
                });
            }
            return Ok(());
        };

        let expected_index = last_applied.index().checked_add(1).ok_or(
            ControlPlaneError::ControlPlaneLogIndexOverflow {
                index: last_applied.index(),
            },
        )?;
        if raft_log_id.index() != expected_index {
            return Err(ControlPlaneError::ControlPlaneLogIndexMismatch {
                expected_index,
                actual_index: raft_log_id.index(),
            });
        }
        if raft_log_id.committed_leader_id().term < last_applied.committed_leader_id().term {
            return Err(ControlPlaneError::ControlPlaneLogTermRegression {
                previous_term: last_applied.committed_leader_id().term,
                actual_term: raft_log_id.committed_leader_id().term,
                index: raft_log_id.index(),
            });
        }
        if raft_log_id.committed_leader_id().term == last_applied.committed_leader_id().term
            && raft_log_id.committed_leader_id().node_id
                < last_applied.committed_leader_id().node_id
        {
            return Err(ControlPlaneError::CommandDecode {
                message: format!(
                    "OpenRaft log id {raft_log_id} is not after last applied log id {last_applied}"
                ),
            });
        }
        Ok(())
    }

    pub fn build_snapshot(
        &mut self,
    ) -> Result<SnapshotOf<ControlPlaneRaftTypeConfig>, ControlPlaneError> {
        let artifact = self.inner.build_snapshot_artifact()?;
        let meta = self.snapshot_meta_for_artifact(&artifact)?;
        let snapshot = Snapshot {
            meta,
            snapshot: Cursor::new(artifact.into_payload()),
        };
        self.current_snapshot = Some(snapshot.clone());
        Ok(snapshot)
    }

    pub fn create_snapshot_builder(
        &mut self,
    ) -> Result<ControlPlaneRaftSnapshotBuilder, ControlPlaneError> {
        Ok(ControlPlaneRaftSnapshotBuilder::new(self.build_snapshot()?))
    }

    pub fn install_snapshot(
        &mut self,
        meta: &SnapshotMetaOf<ControlPlaneRaftTypeConfig>,
        snapshot: Cursor<Vec<u8>>,
    ) -> Result<(), ControlPlaneError> {
        let last_applied = self.validate_snapshot_meta(meta)?;
        let payload = snapshot.into_inner();
        self.inner
            .install_snapshot_artifact(ControlPlaneSnapshotArtifact::new(
                last_applied,
                payload.clone(),
            ))?;
        self.last_applied = meta.last_log_id;
        self.last_membership = meta.last_membership.clone();
        self.current_snapshot = Some(Snapshot {
            meta: meta.clone(),
            snapshot: Cursor::new(payload),
        });
        Ok(())
    }

    fn snapshot_meta_for_artifact(
        &self,
        artifact: &ControlPlaneSnapshotArtifact,
    ) -> Result<SnapshotMetaOf<ControlPlaneRaftTypeConfig>, ControlPlaneError> {
        let last_log_id = match artifact.last_applied() {
            None => match self.last_applied {
                Some(last_applied) if is_openraft_bootstrap_log_id(last_applied) => {
                    Some(last_applied)
                }
                Some(last_applied) => {
                    return Err(ControlPlaneError::SnapshotDecode {
                        message: format!(
                            "snapshot artifact has no control-plane last-applied log id but OpenRaft log id is {last_applied}"
                        ),
                    });
                }
                None => None,
            },
            Some(artifact_log_id) => {
                let last_applied = self.last_applied.ok_or_else(|| {
                    ControlPlaneError::SnapshotDecode {
                        message: format!(
                            "snapshot artifact has last-applied {artifact_log_id:?} but OpenRaft log id is absent"
                        ),
                    }
                })?;
                if control_plane_log_id_from_raft(last_applied) != Some(artifact_log_id) {
                    return Err(ControlPlaneError::SnapshotDecode {
                        message: format!(
                            "snapshot artifact last-applied {artifact_log_id:?} does not match OpenRaft log id {last_applied}"
                        ),
                    });
                }
                Some(last_applied)
            }
        };
        Ok(SnapshotMeta {
            last_log_id,
            last_membership: self.last_membership.clone(),
            snapshot_id: Self::snapshot_id_for_log_id(last_log_id),
        })
    }

    fn snapshot_id_for_log_id(log_id: Option<LogIdOf<ControlPlaneRaftTypeConfig>>) -> String {
        match log_id {
            Some(log_id) => format!(
                "control-plane-T{}-N{}-I{}",
                log_id.committed_leader_id().term,
                log_id.committed_leader_id().node_id,
                log_id.index()
            ),
            None => "control-plane-empty".to_string(),
        }
    }

    fn validate_snapshot_meta(
        &self,
        meta: &SnapshotMetaOf<ControlPlaneRaftTypeConfig>,
    ) -> Result<Option<ControlPlaneLogId>, ControlPlaneError> {
        let snapshot_log_id = match meta.last_log_id {
            Some(log_id) if is_openraft_bootstrap_log_id(log_id) => None,
            Some(log_id) => Some(Self::validate_snapshot_log_id_shape("last_log_id", log_id)?),
            None => None,
        };
        self.validate_snapshot_install_position(meta.last_log_id)?;
        Self::validate_snapshot_membership_position(meta.last_log_id, &meta.last_membership)?;
        let expected_snapshot_id = Self::snapshot_id_for_log_id(meta.last_log_id);
        if meta.snapshot_id != expected_snapshot_id {
            return Err(ControlPlaneError::SnapshotDecode {
                message: format!(
                    "OpenRaft snapshot id {} does not match expected {} for last_log_id {:?}",
                    meta.snapshot_id, expected_snapshot_id, meta.last_log_id
                ),
            });
        }
        Ok(snapshot_log_id)
    }

    fn validate_restart_log_id_consistency(
        inner: &ReplicatedControlPlaneStateMachine,
        last_applied: Option<LogIdOf<ControlPlaneRaftTypeConfig>>,
    ) -> Result<(), ControlPlaneError> {
        match (inner.last_applied(), last_applied) {
            (None, None) => Ok(()),
            (None, Some(log_id)) if is_openraft_bootstrap_log_id(log_id) => Ok(()),
            (None, Some(log_id)) => Err(ControlPlaneError::SnapshotDecode {
                message: format!(
                    "OpenRaft state machine restart has log id {log_id} but no control-plane last-applied log id"
                ),
            }),
            (Some(control_plane_log_id), Some(log_id))
                if control_plane_log_id_from_raft(log_id) == Some(control_plane_log_id) =>
            {
                Ok(())
            }
            (Some(control_plane_log_id), Some(log_id)) => Err(ControlPlaneError::SnapshotDecode {
                message: format!(
                    "OpenRaft state machine restart log id {log_id} does not match control-plane last-applied {control_plane_log_id:?}"
                ),
            }),
            (Some(control_plane_log_id), None) => Err(ControlPlaneError::SnapshotDecode {
                message: format!(
                    "OpenRaft state machine restart is missing log id for control-plane last-applied {control_plane_log_id:?}"
                ),
            }),
        }
    }

    fn validate_snapshot_log_id_shape(
        field: &'static str,
        log_id: LogIdOf<ControlPlaneRaftTypeConfig>,
    ) -> Result<ControlPlaneLogId, ControlPlaneError> {
        control_plane_log_id_from_raft(log_id).ok_or_else(|| ControlPlaneError::SnapshotDecode {
            message: format!("invalid OpenRaft snapshot {field}: {log_id}"),
        })
    }

    fn validate_snapshot_install_position(
        &self,
        snapshot_last_log_id: Option<LogIdOf<ControlPlaneRaftTypeConfig>>,
    ) -> Result<(), ControlPlaneError> {
        let Some(current_last_applied) = self.last_applied else {
            return Ok(());
        };
        let Some(snapshot_last_log_id) = snapshot_last_log_id else {
            return Err(ControlPlaneError::ControlPlaneSnapshotMissingLogId {
                current_index: current_last_applied.index(),
            });
        };
        if snapshot_last_log_id.index() < current_last_applied.index() {
            return Err(ControlPlaneError::ControlPlaneSnapshotLogIndexRegression {
                current_index: current_last_applied.index(),
                artifact_index: snapshot_last_log_id.index(),
            });
        }
        if snapshot_last_log_id.index() == current_last_applied.index()
            && snapshot_last_log_id != current_last_applied
        {
            return Err(ControlPlaneError::SnapshotDecode {
                message: format!(
                    "OpenRaft snapshot last_log_id {snapshot_last_log_id} does not match current applied log id {current_last_applied}"
                ),
            });
        }
        if snapshot_last_log_id.committed_leader_id().term
            < current_last_applied.committed_leader_id().term
        {
            return Err(ControlPlaneError::ControlPlaneLogTermRegression {
                previous_term: current_last_applied.committed_leader_id().term,
                actual_term: snapshot_last_log_id.committed_leader_id().term,
                index: snapshot_last_log_id.index(),
            });
        }
        if snapshot_last_log_id.committed_leader_id().term
            == current_last_applied.committed_leader_id().term
            && snapshot_last_log_id.committed_leader_id().node_id
                != current_last_applied.committed_leader_id().node_id
        {
            return Err(ControlPlaneError::SnapshotDecode {
                message: format!(
                    "OpenRaft snapshot last_log_id {snapshot_last_log_id} conflicts with current applied leader id {current_last_applied}"
                ),
            });
        }
        Ok(())
    }

    fn validate_snapshot_membership_position(
        snapshot_last_log_id: Option<LogIdOf<ControlPlaneRaftTypeConfig>>,
        membership: &StoredMembershipOf<ControlPlaneRaftTypeConfig>,
    ) -> Result<(), ControlPlaneError> {
        let Some(membership_log_id) = membership.log_id().as_ref().copied() else {
            return Ok(());
        };
        if !is_openraft_bootstrap_log_id(membership_log_id) {
            Self::validate_snapshot_log_id_shape("last_membership.log_id", membership_log_id)?;
        }
        let Some(snapshot_last_log_id) = snapshot_last_log_id else {
            return Err(ControlPlaneError::SnapshotDecode {
                message: format!(
                    "OpenRaft snapshot membership log id {membership_log_id} is present without snapshot last_log_id"
                ),
            });
        };
        if membership_log_id.index() > snapshot_last_log_id.index() {
            return Err(ControlPlaneError::SnapshotDecode {
                message: format!(
                    "OpenRaft snapshot membership log id {membership_log_id} is after snapshot last_log_id {snapshot_last_log_id}"
                ),
            });
        }
        if membership_log_id.index() == snapshot_last_log_id.index()
            && membership_log_id != snapshot_last_log_id
        {
            return Err(ControlPlaneError::SnapshotDecode {
                message: format!(
                    "OpenRaft snapshot membership log id {membership_log_id} does not match snapshot last_log_id {snapshot_last_log_id} at the same index"
                ),
            });
        }
        if membership_log_id.committed_leader_id().term
            > snapshot_last_log_id.committed_leader_id().term
        {
            return Err(ControlPlaneError::SnapshotDecode {
                message: format!(
                    "OpenRaft snapshot membership log id {membership_log_id} has a future term relative to snapshot last_log_id {snapshot_last_log_id}"
                ),
            });
        }
        if membership_log_id.committed_leader_id().term
            == snapshot_last_log_id.committed_leader_id().term
            && membership_log_id.committed_leader_id().node_id
                != snapshot_last_log_id.committed_leader_id().node_id
        {
            return Err(ControlPlaneError::SnapshotDecode {
                message: format!(
                    "OpenRaft snapshot membership log id {membership_log_id} conflicts with snapshot leader id {snapshot_last_log_id}"
                ),
            });
        }
        Ok(())
    }
}

impl RaftStateMachine<ControlPlaneRaftTypeConfig> for ControlPlaneRaftStateMachine {
    type SnapshotBuilder = ControlPlaneRaftSnapshotBuilder;

    async fn applied_state(
        &mut self,
    ) -> Result<
        (
            Option<LogIdOf<ControlPlaneRaftTypeConfig>>,
            StoredMembershipOf<ControlPlaneRaftTypeConfig>,
        ),
        io::Error,
    > {
        Ok(ControlPlaneRaftStateMachine::applied_state(self))
    }

    async fn apply<Strm>(&mut self, mut entries: Strm) -> Result<(), io::Error>
    where
        Strm: Stream<Item = Result<EntryResponder<ControlPlaneRaftTypeConfig>, io::Error>>
            + Unpin
            + OptionalSend,
    {
        while let Some(entry) = entries.next().await {
            let (entry, responder) = entry?;
            let response = self
                .apply_entry(entry)
                .map_err(|error| control_plane_error_to_io_error("OpenRaft apply", error))?;
            if let Some(responder) = responder {
                responder.send(response);
            }
        }
        Ok(())
    }

    async fn try_create_snapshot_builder(&mut self, _force: bool) -> Option<Self::SnapshotBuilder> {
        Some(self.get_snapshot_builder().await)
    }

    async fn get_snapshot_builder(&mut self) -> Self::SnapshotBuilder {
        match self.create_snapshot_builder() {
            Ok(builder) => builder,
            Err(error) => ControlPlaneRaftSnapshotBuilder::from_error(error),
        }
    }

    async fn begin_receiving_snapshot(&mut self) -> Result<Cursor<Vec<u8>>, io::Error> {
        Ok(Cursor::new(Vec::new()))
    }

    async fn install_snapshot(
        &mut self,
        meta: &SnapshotMetaOf<ControlPlaneRaftTypeConfig>,
        snapshot: Cursor<Vec<u8>>,
    ) -> Result<(), io::Error> {
        ControlPlaneRaftStateMachine::install_snapshot(self, meta, snapshot)
            .map_err(|error| control_plane_error_to_io_error("OpenRaft install snapshot", error))
    }

    async fn get_current_snapshot(
        &mut self,
    ) -> Result<Option<SnapshotOf<ControlPlaneRaftTypeConfig>>, io::Error> {
        Ok(self.current_snapshot.clone())
    }
}

#[cfg(test)]
mod tests {
    use std::collections::{BTreeMap, BTreeSet};
    use std::future::Future;
    use std::sync::Arc;
    use std::time::Duration;

    use super::*;
    use futures_util::stream;
    use openraft::errors::{
        NetworkError, RPCError, ReplicationClosed, StreamingError, Unreachable,
    };
    use openraft::network::{RPCOption, RaftNetworkFactory, RaftNetworkV2};
    use openraft::raft::{
        AppendEntriesRequest, AppendEntriesResponse, SnapshotResponse, TransferLeaderRequest,
        TransferLeaderResponse, VoteRequest, VoteResponse,
    };
    use openraft::type_config::TypeConfigExt;
    use openraft::{AnyError, Config, Membership, Raft, ReadPolicy, ServerState};

    use crate::control_plane::{
        ClusterControlSnapshot, NodeAvailabilityState, RuntimeMapFreshnessProof,
    };
    use crate::types::PgId;

    #[derive(Debug, Clone, Copy, Default)]
    struct UnreachableRaftNetworkFactory;

    impl RaftNetworkFactory<ControlPlaneRaftTypeConfig> for UnreachableRaftNetworkFactory {
        type Network = UnreachableRaftNetwork;

        async fn new_client(
            &mut self,
            target: ControlPlaneRaftNodeId,
            _node: &BasicNode,
        ) -> Self::Network {
            UnreachableRaftNetwork { target }
        }
    }

    #[derive(Debug, Clone, Copy)]
    struct UnreachableRaftNetwork {
        target: ControlPlaneRaftNodeId,
    }

    impl UnreachableRaftNetwork {
        fn unreachable(&self, rpc_name: &'static str) -> Unreachable<ControlPlaneRaftTypeConfig> {
            Unreachable::new(&AnyError::error(format!(
                "test network should not send {rpc_name} to node {}",
                self.target
            )))
        }
    }

    impl RaftNetworkV2<ControlPlaneRaftTypeConfig> for UnreachableRaftNetwork {
        async fn append_entries(
            &mut self,
            _rpc: AppendEntriesRequest<ControlPlaneRaftTypeConfig>,
            _option: RPCOption,
        ) -> Result<
            AppendEntriesResponse<ControlPlaneRaftTypeConfig>,
            RPCError<ControlPlaneRaftTypeConfig>,
        > {
            Err(RPCError::Unreachable(self.unreachable("append_entries")))
        }

        async fn vote(
            &mut self,
            _rpc: VoteRequest<ControlPlaneRaftTypeConfig>,
            _option: RPCOption,
        ) -> Result<VoteResponse<ControlPlaneRaftTypeConfig>, RPCError<ControlPlaneRaftTypeConfig>>
        {
            Err(RPCError::Unreachable(self.unreachable("vote")))
        }

        async fn full_snapshot(
            &mut self,
            _vote: VoteOf<ControlPlaneRaftTypeConfig>,
            _snapshot: SnapshotOf<ControlPlaneRaftTypeConfig>,
            _cancel: impl Future<Output = ReplicationClosed> + OptionalSend + 'static,
            _option: RPCOption,
        ) -> Result<
            SnapshotResponse<ControlPlaneRaftTypeConfig>,
            StreamingError<ControlPlaneRaftTypeConfig>,
        > {
            Err(StreamingError::Unreachable(
                self.unreachable("full_snapshot"),
            ))
        }
    }

    #[derive(Debug, Clone, Default)]
    struct InMemoryRaftNetworkFactory {
        peers: Arc<
            Mutex<
                BTreeMap<
                    ControlPlaneRaftNodeId,
                    Raft<ControlPlaneRaftTypeConfig, ControlPlaneRaftStateMachine>,
                >,
            >,
        >,
    }

    impl InMemoryRaftNetworkFactory {
        fn register(
            &self,
            node_id: ControlPlaneRaftNodeId,
            raft: Raft<ControlPlaneRaftTypeConfig, ControlPlaneRaftStateMachine>,
        ) {
            self.peers.lock().unwrap().insert(node_id, raft);
        }

        fn unregister(&self, node_id: ControlPlaneRaftNodeId) {
            self.peers.lock().unwrap().remove(&node_id);
        }
    }

    impl RaftNetworkFactory<ControlPlaneRaftTypeConfig> for InMemoryRaftNetworkFactory {
        type Network = InMemoryRaftNetwork;

        async fn new_client(
            &mut self,
            target: ControlPlaneRaftNodeId,
            _node: &BasicNode,
        ) -> Self::Network {
            InMemoryRaftNetwork {
                peers: self.peers.clone(),
                target,
            }
        }
    }

    #[derive(Clone)]
    struct InMemoryRaftNetwork {
        peers: Arc<
            Mutex<
                BTreeMap<
                    ControlPlaneRaftNodeId,
                    Raft<ControlPlaneRaftTypeConfig, ControlPlaneRaftStateMachine>,
                >,
            >,
        >,
        target: ControlPlaneRaftNodeId,
    }

    impl InMemoryRaftNetwork {
        fn target_raft(
            &self,
            rpc_name: &'static str,
        ) -> Result<
            Raft<ControlPlaneRaftTypeConfig, ControlPlaneRaftStateMachine>,
            RPCError<ControlPlaneRaftTypeConfig>,
        > {
            let peers = self.peers.lock().map_err(|_| {
                RPCError::Network(NetworkError::from_string(
                    "in-memory test raft network registry lock poisoned",
                ))
            })?;
            peers.get(&self.target).cloned().ok_or_else(|| {
                RPCError::Unreachable(Unreachable::new(&AnyError::error(format!(
                    "in-memory test raft network has no target {} for {rpc_name}",
                    self.target
                ))))
            })
        }

        fn remote_failure(
            &self,
            rpc_name: &'static str,
            error: impl fmt::Display,
        ) -> RPCError<ControlPlaneRaftTypeConfig> {
            RPCError::Network(NetworkError::from_string(format!(
                "in-memory test raft network {rpc_name} to node {} failed: {error}",
                self.target
            )))
        }
    }

    impl fmt::Debug for InMemoryRaftNetwork {
        fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            f.debug_struct("InMemoryRaftNetwork")
                .field("target", &self.target)
                .finish_non_exhaustive()
        }
    }

    impl RaftNetworkV2<ControlPlaneRaftTypeConfig> for InMemoryRaftNetwork {
        async fn append_entries(
            &mut self,
            rpc: AppendEntriesRequest<ControlPlaneRaftTypeConfig>,
            _option: RPCOption,
        ) -> Result<
            AppendEntriesResponse<ControlPlaneRaftTypeConfig>,
            RPCError<ControlPlaneRaftTypeConfig>,
        > {
            self.target_raft("append_entries")?
                .append_entries(rpc)
                .await
                .map_err(|error| self.remote_failure("append_entries", error))
        }

        async fn vote(
            &mut self,
            rpc: VoteRequest<ControlPlaneRaftTypeConfig>,
            _option: RPCOption,
        ) -> Result<VoteResponse<ControlPlaneRaftTypeConfig>, RPCError<ControlPlaneRaftTypeConfig>>
        {
            self.target_raft("vote")?
                .vote(rpc)
                .await
                .map_err(|error| self.remote_failure("vote", error))
        }

        async fn pre_vote(
            &mut self,
            rpc: VoteRequest<ControlPlaneRaftTypeConfig>,
            _option: RPCOption,
        ) -> Result<VoteResponse<ControlPlaneRaftTypeConfig>, RPCError<ControlPlaneRaftTypeConfig>>
        {
            self.target_raft("pre_vote")?
                .pre_vote(rpc)
                .await
                .map_err(|error| self.remote_failure("pre_vote", error))
        }

        async fn full_snapshot(
            &mut self,
            vote: VoteOf<ControlPlaneRaftTypeConfig>,
            snapshot: SnapshotOf<ControlPlaneRaftTypeConfig>,
            _cancel: impl Future<Output = ReplicationClosed> + OptionalSend + 'static,
            _option: RPCOption,
        ) -> Result<
            SnapshotResponse<ControlPlaneRaftTypeConfig>,
            StreamingError<ControlPlaneRaftTypeConfig>,
        > {
            self.target_raft("full_snapshot")?
                .install_full_snapshot(vote, snapshot)
                .await
                .map_err(|error| {
                    StreamingError::Network(NetworkError::from_string(format!(
                        "in-memory test raft network full_snapshot to node {} failed: {error}",
                        self.target
                    )))
                })
        }

        async fn transfer_leader(
            &mut self,
            req: TransferLeaderRequest<ControlPlaneRaftTypeConfig>,
            _option: RPCOption,
        ) -> Result<
            TransferLeaderResponse<ControlPlaneRaftTypeConfig>,
            RPCError<ControlPlaneRaftTypeConfig>,
        > {
            self.target_raft("transfer_leader")?
                .handle_transfer_leader(req)
                .await
                .map_err(|error| self.remote_failure("transfer_leader", error))
        }
    }

    fn test_raft_config(cluster_name: &'static str) -> Arc<Config> {
        test_raft_config_with_log_reversion(cluster_name, None)
    }

    fn test_raft_config_with_log_reversion(
        cluster_name: &'static str,
        allow_log_reversion: Option<bool>,
    ) -> Arc<Config> {
        Arc::new(
            Config {
                cluster_name: cluster_name.to_string(),
                heartbeat_interval: 50,
                election_timeout_min: 150,
                election_timeout_max: 300,
                enable_tick: false,
                enable_heartbeat: false,
                enable_elect: false,
                allow_log_reversion,
                ..Default::default()
            }
            .validate()
            .unwrap(),
        )
    }

    async fn wait_for_local_leader(
        raft: &Raft<ControlPlaneRaftTypeConfig, ControlPlaneRaftStateMachine>,
        message: &'static str,
    ) {
        raft.wait(Some(Duration::from_secs(1)))
            .state(ServerState::Leader, message)
            .await
            .unwrap();
        raft.as_leader()
            .expect("local single-node raft should have a committed leader vote");
    }

    async fn initialized_two_node_authorities(
        cluster_name: &'static str,
        node1: ControlPlaneRaftNodeId,
        node2: ControlPlaneRaftNodeId,
    ) -> (ControlPlaneRaftAuthority, ControlPlaneRaftAuthority) {
        let network = InMemoryRaftNetworkFactory::default();
        let config = test_raft_config(cluster_name);
        let raft1 = Raft::<ControlPlaneRaftTypeConfig, ControlPlaneRaftStateMachine>::new(
            node1,
            config.clone(),
            network.clone(),
            ControlPlaneRaftLogStore::empty(),
            ControlPlaneRaftStateMachine::empty(),
        )
        .await
        .unwrap();
        let raft2 = Raft::<ControlPlaneRaftTypeConfig, ControlPlaneRaftStateMachine>::new(
            node2,
            config,
            network.clone(),
            ControlPlaneRaftLogStore::empty(),
            ControlPlaneRaftStateMachine::empty(),
        )
        .await
        .unwrap();
        network.register(node1, raft1.clone());
        network.register(node2, raft2.clone());
        let authority1 = ControlPlaneRaftAuthority::new(raft1);
        let authority2 = ControlPlaneRaftAuthority::new(raft2);

        authority1
            .initialize_membership(BTreeMap::from([
                (node1, BasicNode::new(format!("node-{node1}"))),
                (node2, BasicNode::new(format!("node-{node2}"))),
            ]))
            .await
            .unwrap();
        wait_for_local_leader(authority1.raft(), "two-node initialized leadership").await;

        (authority1, authority2)
    }

    async fn initialized_three_node_cluster_with_two_voters(
        cluster_name: &'static str,
        node1: ControlPlaneRaftNodeId,
        node2: ControlPlaneRaftNodeId,
        node3: ControlPlaneRaftNodeId,
    ) -> (
        ControlPlaneRaftAuthority,
        ControlPlaneRaftAuthority,
        ControlPlaneRaftAuthority,
    ) {
        let network = InMemoryRaftNetworkFactory::default();
        let config = test_raft_config(cluster_name);
        let raft1 = Raft::<ControlPlaneRaftTypeConfig, ControlPlaneRaftStateMachine>::new(
            node1,
            config.clone(),
            network.clone(),
            ControlPlaneRaftLogStore::empty(),
            ControlPlaneRaftStateMachine::empty(),
        )
        .await
        .unwrap();
        let raft2 = Raft::<ControlPlaneRaftTypeConfig, ControlPlaneRaftStateMachine>::new(
            node2,
            config.clone(),
            network.clone(),
            ControlPlaneRaftLogStore::empty(),
            ControlPlaneRaftStateMachine::empty(),
        )
        .await
        .unwrap();
        let raft3 = Raft::<ControlPlaneRaftTypeConfig, ControlPlaneRaftStateMachine>::new(
            node3,
            config,
            network.clone(),
            ControlPlaneRaftLogStore::empty(),
            ControlPlaneRaftStateMachine::empty(),
        )
        .await
        .unwrap();
        network.register(node1, raft1.clone());
        network.register(node2, raft2.clone());
        network.register(node3, raft3.clone());
        let authority1 = ControlPlaneRaftAuthority::new(raft1);
        let authority2 = ControlPlaneRaftAuthority::new(raft2);
        let authority3 = ControlPlaneRaftAuthority::new(raft3);

        authority1
            .initialize_membership(BTreeMap::from([
                (node1, BasicNode::new(format!("node-{node1}"))),
                (node2, BasicNode::new(format!("node-{node2}"))),
            ]))
            .await
            .unwrap();
        wait_for_local_leader(authority1.raft(), "three-node initialized leadership").await;

        (authority1, authority2, authority3)
    }

    struct ThreeVoterAuthorityFixture {
        network: InMemoryRaftNetworkFactory,
        config: Arc<Config>,
        leader_log_store: ControlPlaneRaftLogStore,
        third_log_store: ControlPlaneRaftLogStore,
        authority1: ControlPlaneRaftAuthority,
        authority2: ControlPlaneRaftAuthority,
        authority3: ControlPlaneRaftAuthority,
    }

    async fn initialized_three_node_voter_authorities(
        cluster_name: &'static str,
        node1: ControlPlaneRaftNodeId,
        node2: ControlPlaneRaftNodeId,
        node3: ControlPlaneRaftNodeId,
    ) -> ThreeVoterAuthorityFixture {
        initialized_three_node_voter_authorities_with_config(
            test_raft_config(cluster_name),
            node1,
            node2,
            node3,
        )
        .await
    }

    async fn initialized_three_node_voter_authorities_with_config(
        config: Arc<Config>,
        node1: ControlPlaneRaftNodeId,
        node2: ControlPlaneRaftNodeId,
        node3: ControlPlaneRaftNodeId,
    ) -> ThreeVoterAuthorityFixture {
        let network = InMemoryRaftNetworkFactory::default();
        let leader_log_store = ControlPlaneRaftLogStore::empty();
        let raft1 = Raft::<ControlPlaneRaftTypeConfig, ControlPlaneRaftStateMachine>::new(
            node1,
            config.clone(),
            network.clone(),
            leader_log_store.clone(),
            ControlPlaneRaftStateMachine::empty(),
        )
        .await
        .unwrap();
        let raft2 = Raft::<ControlPlaneRaftTypeConfig, ControlPlaneRaftStateMachine>::new(
            node2,
            config.clone(),
            network.clone(),
            ControlPlaneRaftLogStore::empty(),
            ControlPlaneRaftStateMachine::empty(),
        )
        .await
        .unwrap();
        let log_store3 = ControlPlaneRaftLogStore::empty();
        let raft3 = Raft::<ControlPlaneRaftTypeConfig, ControlPlaneRaftStateMachine>::new(
            node3,
            config.clone(),
            network.clone(),
            log_store3.clone(),
            ControlPlaneRaftStateMachine::empty(),
        )
        .await
        .unwrap();
        network.register(node1, raft1.clone());
        network.register(node2, raft2.clone());
        network.register(node3, raft3.clone());
        let authority1 =
            ControlPlaneRaftAuthority::new_with_log_store(raft1, leader_log_store.clone());
        let authority2 = ControlPlaneRaftAuthority::new(raft2);
        let authority3 = ControlPlaneRaftAuthority::new_with_log_store(raft3, log_store3.clone());

        authority1
            .initialize_membership(BTreeMap::from([
                (node1, BasicNode::new(format!("node-{node1}"))),
                (node2, BasicNode::new(format!("node-{node2}"))),
                (node3, BasicNode::new(format!("node-{node3}"))),
            ]))
            .await
            .unwrap();
        wait_for_local_leader(authority1.raft(), "three-voter initialized leadership").await;

        ThreeVoterAuthorityFixture {
            network,
            config,
            leader_log_store,
            third_log_store: log_store3,
            authority1,
            authority2,
            authority3,
        }
    }

    async fn capture_openraft_restart_artifact(
        log_store: &ControlPlaneRaftLogStore,
        authority: &ControlPlaneRaftAuthority,
    ) -> ControlPlaneRaftRestartArtifact {
        let log_store = log_store.export_restart_artifact().unwrap();
        let state_machine = authority
            .raft()
            .with_state_machine(|state_machine| {
                let artifact = state_machine.export_restart_artifact();
                Box::pin(async move { artifact })
            })
            .await
            .unwrap();
        ControlPlaneRaftRestartArtifact {
            log_store,
            state_machine,
        }
    }

    async fn wait_for_log_purged_to(
        log_store: &ControlPlaneRaftLogStore,
        log_id: LogIdOf<ControlPlaneRaftTypeConfig>,
        message: &'static str,
    ) {
        for _ in 0..100 {
            if log_store.last_purged_log_id().unwrap() == Some(log_id) {
                return;
            }
            ControlPlaneRaftTypeConfig::sleep(Duration::from_millis(10)).await;
        }
        panic!("{message}: log store did not purge through {log_id}");
    }

    fn raft_log_id(term: u64, node_id: u64, index: u64) -> LogIdOf<ControlPlaneRaftTypeConfig> {
        LogId::new(LeaderId { term, node_id }, index)
    }

    fn blank_entry(term: u64, node_id: u64, index: u64) -> ControlPlaneRaftEntry {
        Entry {
            log_id: raft_log_id(term, node_id, index),
            payload: EntryPayload::Blank,
        }
    }

    fn membership_entry(term: u64, node_id: u64, index: u64) -> ControlPlaneRaftEntry {
        Entry {
            log_id: raft_log_id(term, node_id, index),
            payload: EntryPayload::Membership(Membership::new_with_defaults(
                vec![BTreeSet::from([1, 2])],
                [],
            )),
        }
    }

    fn bootstrap_membership_entry(node_id: u64) -> ControlPlaneRaftEntry {
        membership_entry(0, node_id, 0)
    }

    fn normal_entry(
        term: u64,
        node_id: u64,
        index: u64,
        command: ControlPlaneCommand,
    ) -> ControlPlaneRaftEntry {
        Entry {
            log_id: raft_log_id(term, node_id, index),
            payload: EntryPayload::Normal(command),
        }
    }

    fn test_membership() -> Membership<ControlPlaneRaftNodeId, BasicNode> {
        Membership::new_with_defaults(vec![BTreeSet::from([1, 2])], [])
    }

    fn replicated_state_machine_with_noops(
        term: u64,
        through_index: u64,
    ) -> ReplicatedControlPlaneStateMachine {
        let mut state_machine = ReplicatedControlPlaneStateMachine::empty();
        for index in 1..=through_index {
            state_machine
                .apply_committed_noop(ControlPlaneLogId::new(term, index).unwrap())
                .unwrap();
        }
        state_machine
    }

    fn state_machine_restart_artifact_with_noops(
        term: u64,
        node_id: u64,
        through_index: u64,
    ) -> ControlPlaneRaftStateMachineRestartArtifact {
        ControlPlaneRaftStateMachineRestartArtifact {
            inner: replicated_state_machine_with_noops(term, through_index),
            last_applied: Some(raft_log_id(term, node_id, through_index)),
            last_membership: StoredMembership::default(),
        }
    }

    #[test]
    fn control_plane_raft_log_id_round_trips_term_and_index() {
        let control_plane_log_id = ControlPlaneLogId::new(7, 42).unwrap();
        let raft_log_id = raft_log_id_from_control_plane(3, control_plane_log_id);

        assert_eq!(raft_log_id.committed_leader_id().term, 7);
        assert_eq!(raft_log_id.committed_leader_id().node_id, 3);
        assert_eq!(raft_log_id.index(), 42);
        assert_eq!(
            control_plane_log_id_from_raft(raft_log_id),
            Some(control_plane_log_id)
        );
    }

    #[test]
    fn control_plane_raft_log_id_rejects_reserved_values() {
        let zero_term = LogId::new(
            LeaderId {
                term: 0,
                node_id: 1,
            },
            1,
        );
        let zero_index = LogId::new(
            LeaderId {
                term: 1,
                node_id: 1,
            },
            0,
        );

        assert_eq!(control_plane_log_id_from_raft(zero_term), None);
        assert_eq!(control_plane_log_id_from_raft(zero_index), None);
    }

    #[test]
    fn control_plane_raft_node_id_conversion_is_bounded_by_storage_node_id() {
        let storage_id = NodeId::new(17);

        assert_eq!(raft_node_id_from_storage_node_id(storage_id), 17);
        assert_eq!(storage_node_id_from_raft_node_id(17), Some(storage_id));
        assert_eq!(
            storage_node_id_from_raft_node_id(u64::from(u32::MAX) + 1),
            None
        );
    }

    #[test]
    fn control_plane_raft_state_machine_applies_blank_and_membership_entries() {
        let mut state_machine = ControlPlaneRaftStateMachine::empty();

        assert!(matches!(
            state_machine.apply_entry(blank_entry(1, 1, 1)).unwrap(),
            ControlPlaneRaftApplyResponse::Blank
        ));
        assert_eq!(state_machine.last_applied(), Some(raft_log_id(1, 1, 1)));

        assert!(matches!(
            state_machine
                .apply_entry(membership_entry(1, 1, 2))
                .unwrap(),
            ControlPlaneRaftApplyResponse::Membership
        ));
        assert_eq!(state_machine.last_applied(), Some(raft_log_id(1, 1, 2)));
        assert_eq!(
            state_machine.last_membership().log_id(),
            &Some(raft_log_id(1, 1, 2))
        );
        assert_eq!(
            state_machine
                .inner()
                .last_applied()
                .map(|log_id| (log_id.term(), log_id.index())),
            Some((1, 2))
        );
    }

    #[test]
    fn control_plane_raft_state_machine_applies_openraft_bootstrap_membership() {
        let mut empty_state_machine = ControlPlaneRaftStateMachine::empty();
        let empty_snapshot = empty_state_machine.build_snapshot().unwrap();
        assert_eq!(empty_snapshot.meta.last_log_id, None);
        assert_eq!(empty_snapshot.meta.snapshot_id, "control-plane-empty");

        let mut state_machine = ControlPlaneRaftStateMachine::empty();

        assert!(matches!(
            state_machine
                .apply_entry(bootstrap_membership_entry(7))
                .unwrap(),
            ControlPlaneRaftApplyResponse::Membership
        ));
        assert_eq!(state_machine.last_applied(), Some(raft_log_id(0, 7, 0)));
        assert_eq!(
            state_machine.last_membership().log_id(),
            &Some(raft_log_id(0, 7, 0))
        );
        assert_eq!(state_machine.inner().last_applied(), None);

        let snapshot = state_machine.build_snapshot().unwrap();
        assert_eq!(snapshot.meta.last_log_id, Some(raft_log_id(0, 7, 0)));
        assert_eq!(snapshot.meta.snapshot_id, "control-plane-T0-N7-I0");
        assert_ne!(snapshot.meta.snapshot_id, empty_snapshot.meta.snapshot_id);
        assert_eq!(
            snapshot.meta.last_membership.log_id(),
            &Some(raft_log_id(0, 7, 0))
        );

        let mut target = ControlPlaneRaftStateMachine::empty();
        target
            .install_snapshot(&snapshot.meta, snapshot.snapshot)
            .unwrap();
        assert_eq!(target.last_applied(), Some(raft_log_id(0, 7, 0)));
        assert_eq!(target.inner().last_applied(), None);
        assert_eq!(
            target.last_membership().log_id(),
            &Some(raft_log_id(0, 7, 0))
        );
    }

    #[test]
    fn control_plane_raft_state_machine_new_validates_restart_state() {
        let empty = ControlPlaneRaftStateMachine::new(
            ReplicatedControlPlaneStateMachine::empty(),
            None,
            StoredMembership::default(),
        )
        .unwrap();
        assert_eq!(empty.last_applied(), None);

        let bootstrap = ControlPlaneRaftStateMachine::new(
            ReplicatedControlPlaneStateMachine::empty(),
            Some(raft_log_id(0, 7, 0)),
            StoredMembership::new(Some(raft_log_id(0, 7, 0)), test_membership()),
        )
        .unwrap();
        assert_eq!(bootstrap.last_applied(), Some(raft_log_id(0, 7, 0)));
        assert_eq!(bootstrap.inner().last_applied(), None);

        let applied = ControlPlaneRaftStateMachine::new(
            replicated_state_machine_with_noops(2, 3),
            Some(raft_log_id(2, 7, 3)),
            StoredMembership::new(Some(raft_log_id(2, 7, 2)), test_membership()),
        )
        .unwrap();
        assert_eq!(applied.last_applied(), Some(raft_log_id(2, 7, 3)));
        assert_eq!(
            applied
                .inner()
                .last_applied()
                .map(|log_id| (log_id.term(), log_id.index())),
            Some((2, 3))
        );
    }

    #[test]
    fn control_plane_raft_state_machine_new_rejects_inconsistent_restart_state() {
        let err = ControlPlaneRaftStateMachine::new(
            replicated_state_machine_with_noops(2, 3),
            None,
            StoredMembership::default(),
        )
        .unwrap_err();
        assert!(matches!(err, ControlPlaneError::SnapshotDecode { .. }));

        let err = ControlPlaneRaftStateMachine::new(
            ReplicatedControlPlaneStateMachine::empty(),
            Some(raft_log_id(2, 7, 3)),
            StoredMembership::default(),
        )
        .unwrap_err();
        assert!(matches!(err, ControlPlaneError::SnapshotDecode { .. }));

        let err = ControlPlaneRaftStateMachine::new(
            replicated_state_machine_with_noops(2, 3),
            Some(raft_log_id(3, 7, 3)),
            StoredMembership::default(),
        )
        .unwrap_err();
        assert!(matches!(err, ControlPlaneError::SnapshotDecode { .. }));

        let err = ControlPlaneRaftStateMachine::new(
            replicated_state_machine_with_noops(2, 3),
            Some(raft_log_id(2, 7, 3)),
            StoredMembership::new(Some(raft_log_id(2, 7, 4)), test_membership()),
        )
        .unwrap_err();
        assert!(matches!(err, ControlPlaneError::SnapshotDecode { .. }));
    }

    #[test]
    fn control_plane_raft_state_machine_restores_restart_artifact() {
        let mut source = ControlPlaneRaftStateMachine::empty();
        source
            .apply_entry(normal_entry(
                2,
                7,
                1,
                ControlPlaneCommand::BootstrapInitialClusterMap {
                    nodes: vec![(NodeId::new(1), "node-1".to_string())],
                    pg_ids: vec![PgId::new(0)],
                },
            ))
            .unwrap();
        source.apply_entry(membership_entry(2, 7, 2)).unwrap();
        assert!(matches!(
            source
                .apply_entry(normal_entry(
                    2,
                    7,
                    3,
                    ControlPlaneCommand::MarkNodeAvailability {
                        node_id: NodeId::new(99),
                        availability: NodeAvailabilityState::Healthy,
                    },
                ))
                .unwrap(),
            ControlPlaneRaftApplyResponse::Rejected(ControlPlaneError::UnknownNode { node_id: 99 })
        ));

        let artifact = source.export_restart_artifact();
        let mut restored = ControlPlaneRaftStateMachine::from_restart_artifact(artifact).unwrap();

        assert_eq!(restored.last_applied(), Some(raft_log_id(2, 7, 3)));
        assert_eq!(
            restored.last_membership().log_id(),
            &Some(raft_log_id(2, 7, 2))
        );
        assert_eq!(restored.inner().snapshot(), source.inner().snapshot());
        assert!(restored.current_snapshot().is_none());

        restored.apply_entry(blank_entry(2, 7, 4)).unwrap();
        let runtime_map = restored
            .runtime_map_for_applied_read_index(raft_log_id(2, 7, 4), 12_345)
            .unwrap();
        assert_eq!(
            runtime_map.freshness_proof().read_index(),
            Some(ControlPlaneLogId::new(2, 4).unwrap())
        );
    }

    #[test]
    fn control_plane_raft_state_machine_rejects_invalid_restart_artifacts() {
        let inner_with_applied = replicated_state_machine_with_noops(2, 3);
        let err = ControlPlaneRaftStateMachine::from_restart_artifact(
            ControlPlaneRaftStateMachineRestartArtifact {
                inner: inner_with_applied.clone(),
                last_applied: None,
                last_membership: StoredMembership::default(),
            },
        )
        .unwrap_err();
        assert!(matches!(err, ControlPlaneError::SnapshotDecode { .. }));

        let err = ControlPlaneRaftStateMachine::from_restart_artifact(
            ControlPlaneRaftStateMachineRestartArtifact {
                inner: ReplicatedControlPlaneStateMachine::empty(),
                last_applied: Some(raft_log_id(2, 7, 3)),
                last_membership: StoredMembership::default(),
            },
        )
        .unwrap_err();
        assert!(matches!(err, ControlPlaneError::SnapshotDecode { .. }));

        let err = ControlPlaneRaftStateMachine::from_restart_artifact(
            ControlPlaneRaftStateMachineRestartArtifact {
                inner: inner_with_applied,
                last_applied: Some(raft_log_id(2, 7, 3)),
                last_membership: StoredMembership::new(
                    Some(raft_log_id(2, 7, 4)),
                    test_membership(),
                ),
            },
        )
        .unwrap_err();
        assert!(matches!(err, ControlPlaneError::SnapshotDecode { .. }));
    }

    #[test]
    fn control_plane_raft_state_machine_rejects_nonzero_term_index_zero_membership() {
        let mut state_machine = ControlPlaneRaftStateMachine::empty();

        let err = state_machine
            .apply_entry(membership_entry(1, 7, 0))
            .unwrap_err();

        assert!(matches!(err, ControlPlaneError::CommandDecode { .. }));
        assert_eq!(state_machine.last_applied(), None);
        assert_eq!(state_machine.last_membership().log_id(), &None);
        assert_eq!(state_machine.inner().last_applied(), None);
    }

    #[test]
    fn control_plane_raft_state_machine_rejects_out_of_order_apply() {
        let mut state_machine = ControlPlaneRaftStateMachine::empty();

        let err = state_machine.apply_entry(blank_entry(3, 1, 2)).unwrap_err();
        assert!(matches!(
            err,
            ControlPlaneError::ControlPlaneLogIndexMismatch {
                expected_index: 1,
                actual_index: 2,
            }
        ));
        assert_eq!(state_machine.last_applied(), None);

        state_machine
            .apply_entry(bootstrap_membership_entry(1))
            .unwrap();
        let err = state_machine
            .apply_entry(bootstrap_membership_entry(1))
            .unwrap_err();
        assert!(matches!(
            err,
            ControlPlaneError::ControlPlaneLogIndexMismatch {
                expected_index: 1,
                actual_index: 0,
            }
        ));
        assert_eq!(state_machine.last_applied(), Some(raft_log_id(0, 1, 0)));

        state_machine.apply_entry(blank_entry(3, 2, 1)).unwrap();
        let err = state_machine
            .apply_entry(blank_entry(2, 99, 2))
            .unwrap_err();
        assert!(matches!(
            err,
            ControlPlaneError::ControlPlaneLogTermRegression {
                previous_term: 3,
                actual_term: 2,
                index: 2,
            }
        ));
        assert_eq!(state_machine.last_applied(), Some(raft_log_id(3, 2, 1)));

        let err = state_machine.apply_entry(blank_entry(3, 1, 2)).unwrap_err();
        assert!(matches!(err, ControlPlaneError::CommandDecode { .. }));
        assert_eq!(state_machine.last_applied(), Some(raft_log_id(3, 2, 1)));
    }

    #[test]
    fn control_plane_raft_state_machine_rejects_apply_after_max_index() {
        let inner = ReplicatedControlPlaneStateMachine::new(
            ClusterControlSnapshot::empty(),
            Some(ControlPlaneLogId::new(1, u64::MAX).unwrap()),
        );
        let mut state_machine = ControlPlaneRaftStateMachine::new(
            inner,
            Some(raft_log_id(1, 7, u64::MAX)),
            StoredMembership::default(),
        )
        .unwrap();

        let err = state_machine
            .apply_entry(blank_entry(1, 7, u64::MAX))
            .unwrap_err();

        assert!(matches!(
            err,
            ControlPlaneError::ControlPlaneLogIndexOverflow { index: u64::MAX }
        ));
        assert_eq!(
            state_machine.last_applied(),
            Some(raft_log_id(1, 7, u64::MAX))
        );
    }

    #[test]
    fn control_plane_raft_state_machine_builds_and_installs_snapshot() {
        let mut source = ControlPlaneRaftStateMachine::empty();
        source.apply_entry(blank_entry(2, 7, 1)).unwrap();
        let snapshot = source.build_snapshot().unwrap();

        assert_eq!(snapshot.meta.last_log_id, Some(raft_log_id(2, 7, 1)));
        assert_eq!(snapshot.meta.snapshot_id, "control-plane-T2-N7-I1");

        let mut target = ControlPlaneRaftStateMachine::empty();
        target
            .install_snapshot(&snapshot.meta, snapshot.snapshot)
            .unwrap();

        assert_eq!(target.last_applied(), Some(raft_log_id(2, 7, 1)));
        assert_eq!(
            target
                .inner()
                .last_applied()
                .map(|log_id| (log_id.term(), log_id.index())),
            Some((2, 1))
        );
    }

    #[test]
    fn control_plane_raft_snapshot_builder_returns_stable_snapshot_view() {
        let mut state_machine = ControlPlaneRaftStateMachine::empty();
        state_machine.apply_entry(blank_entry(2, 7, 1)).unwrap();

        let mut builder = state_machine.create_snapshot_builder().unwrap();
        state_machine.apply_entry(blank_entry(2, 7, 2)).unwrap();

        let snapshot = ControlPlaneRaftTypeConfig::run(builder.build_snapshot()).unwrap();

        assert_eq!(snapshot.meta.last_log_id, Some(raft_log_id(2, 7, 1)));
        assert_eq!(snapshot.meta.snapshot_id, "control-plane-T2-N7-I1");
        assert_eq!(state_machine.last_applied(), Some(raft_log_id(2, 7, 2)));
        assert_eq!(
            state_machine
                .current_snapshot()
                .map(|snapshot| snapshot.meta.last_log_id),
            Some(Some(raft_log_id(2, 7, 1)))
        );
    }

    #[test]
    fn control_plane_raft_snapshot_install_rejects_same_position_different_leader() {
        let mut source = ControlPlaneRaftStateMachine::empty();
        source.apply_entry(blank_entry(2, 7, 1)).unwrap();
        let snapshot = source.build_snapshot().unwrap();
        let snapshot_meta = snapshot.meta.clone();
        let snapshot_payload = snapshot.snapshot.clone();

        let mut target = ControlPlaneRaftStateMachine::empty();
        target
            .install_snapshot(&snapshot_meta, snapshot_payload.clone())
            .unwrap();

        let mut bad_meta = snapshot_meta;
        bad_meta.last_log_id = Some(raft_log_id(2, 8, 1));
        let err = target
            .install_snapshot(&bad_meta, snapshot_payload)
            .unwrap_err();

        assert!(matches!(err, ControlPlaneError::SnapshotDecode { .. }));
        assert_eq!(target.last_applied(), Some(raft_log_id(2, 7, 1)));
    }

    #[test]
    fn control_plane_raft_snapshot_install_rejects_mismatched_snapshot_id() {
        let mut source = ControlPlaneRaftStateMachine::empty();
        source.apply_entry(blank_entry(2, 7, 1)).unwrap();
        let snapshot = source.build_snapshot().unwrap();
        let mut bad_meta = snapshot.meta.clone();
        bad_meta.snapshot_id = "control-plane-1".to_string();

        let mut target = ControlPlaneRaftStateMachine::empty();
        let err = target
            .install_snapshot(&bad_meta, snapshot.snapshot)
            .unwrap_err();

        assert!(matches!(err, ControlPlaneError::SnapshotDecode { .. }));
        assert_eq!(target.last_applied(), None);
        assert!(target.current_snapshot().is_none());
    }

    #[test]
    fn control_plane_raft_snapshot_install_rejects_unapplied_membership_log_id() {
        let mut source = ControlPlaneRaftStateMachine::empty();
        source.apply_entry(blank_entry(2, 7, 1)).unwrap();
        let snapshot = source.build_snapshot().unwrap();
        let mut bad_meta = snapshot.meta.clone();
        bad_meta.last_membership =
            StoredMembership::new(Some(raft_log_id(2, 7, 2)), test_membership());

        let mut target = ControlPlaneRaftStateMachine::empty();
        let err = target
            .install_snapshot(&bad_meta, snapshot.snapshot)
            .unwrap_err();

        assert!(matches!(err, ControlPlaneError::SnapshotDecode { .. }));
        assert_eq!(target.last_applied(), None);
    }

    #[test]
    fn control_plane_raft_snapshot_install_rejects_invalid_membership_log_id() {
        let mut source = ControlPlaneRaftStateMachine::empty();
        source.apply_entry(blank_entry(2, 7, 1)).unwrap();
        let snapshot = source.build_snapshot().unwrap();
        let mut bad_meta = snapshot.meta.clone();
        bad_meta.last_membership =
            StoredMembership::new(Some(raft_log_id(0, 7, 1)), test_membership());

        let mut target = ControlPlaneRaftStateMachine::empty();
        let err = target
            .install_snapshot(&bad_meta, snapshot.snapshot)
            .unwrap_err();

        assert!(matches!(err, ControlPlaneError::SnapshotDecode { .. }));
        assert_eq!(target.last_applied(), None);
    }

    #[test]
    fn control_plane_raft_snapshot_install_rejects_nonzero_term_index_zero_log_id() {
        let mut source = ControlPlaneRaftStateMachine::empty();
        source.apply_entry(bootstrap_membership_entry(7)).unwrap();
        let snapshot = source.build_snapshot().unwrap();

        let mut bad_meta = snapshot.meta.clone();
        bad_meta.last_log_id = Some(raft_log_id(1, 7, 0));
        bad_meta.last_membership =
            StoredMembership::new(Some(raft_log_id(1, 7, 0)), test_membership());

        let mut target = ControlPlaneRaftStateMachine::empty();
        let err = target
            .install_snapshot(&bad_meta, snapshot.snapshot)
            .unwrap_err();

        assert!(matches!(err, ControlPlaneError::SnapshotDecode { .. }));
        assert_eq!(target.last_applied(), None);
        assert_eq!(target.last_membership().log_id(), &None);
        assert_eq!(target.inner().last_applied(), None);
    }

    #[test]
    fn control_plane_raft_state_machine_maps_normal_outcomes_to_application_responses() {
        let mut state_machine = ControlPlaneRaftStateMachine::empty();

        let applied = state_machine
            .apply_entry(normal_entry(
                1,
                1,
                1,
                ControlPlaneCommand::BootstrapInitialClusterMap {
                    nodes: vec![(NodeId::new(1), "node-1".to_string())],
                    pg_ids: vec![PgId::new(0)],
                },
            ))
            .unwrap();
        assert!(matches!(
            applied,
            ControlPlaneRaftApplyResponse::Applied(
                ControlPlaneCommandResponse::BootstrapInitialClusterMap
            )
        ));

        let rejected = state_machine
            .apply_entry(normal_entry(
                1,
                1,
                2,
                ControlPlaneCommand::MarkNodeAvailability {
                    node_id: NodeId::new(99),
                    availability: NodeAvailabilityState::Healthy,
                },
            ))
            .unwrap();
        assert!(matches!(
            rejected,
            ControlPlaneRaftApplyResponse::Rejected(ControlPlaneError::UnknownNode { node_id: 99 })
        ));
        assert_eq!(state_machine.last_applied(), Some(raft_log_id(1, 1, 2)));
        assert_eq!(
            state_machine
                .inner()
                .last_applied()
                .map(|log_id| (log_id.term(), log_id.index())),
            Some((1, 2))
        );
    }

    #[test]
    fn control_plane_raft_state_machine_runtime_map_read_index_uses_applied_log_id() {
        let mut state_machine = ControlPlaneRaftStateMachine::empty();
        state_machine
            .apply_entry(normal_entry(
                2,
                7,
                1,
                ControlPlaneCommand::BootstrapInitialClusterMap {
                    nodes: vec![(NodeId::new(1), "node-1".to_string())],
                    pg_ids: vec![PgId::new(0)],
                },
            ))
            .unwrap();

        let runtime_map = state_machine
            .runtime_map_for_applied_read_index(raft_log_id(2, 7, 1), 12_345)
            .unwrap();

        assert_eq!(
            runtime_map.freshness_proof(),
            &RuntimeMapFreshnessProof::ReadIndex {
                authority_incarnation: runtime_map.freshness_proof().authority_incarnation(),
                read_index: ControlPlaneLogId::new(2, 1).unwrap(),
                issued_at_ms: 12_345,
            }
        );
        assert_eq!(
            runtime_map.freshness_proof().read_index(),
            Some(ControlPlaneLogId::new(2, 1).unwrap())
        );
        assert!(runtime_map.freshness_proof().is_serving_authority_read());
    }

    #[test]
    fn control_plane_raft_state_machine_runtime_map_current_read_index_uses_tip() {
        let mut state_machine = ControlPlaneRaftStateMachine::empty();
        state_machine
            .apply_entry(normal_entry(
                2,
                7,
                1,
                ControlPlaneCommand::BootstrapInitialClusterMap {
                    nodes: vec![(NodeId::new(1), "node-1".to_string())],
                    pg_ids: vec![PgId::new(0)],
                },
            ))
            .unwrap();
        state_machine.apply_entry(blank_entry(2, 7, 2)).unwrap();

        let stale_captured_read_index = state_machine
            .runtime_map_for_applied_read_index(raft_log_id(2, 7, 1), 12_345)
            .unwrap_err();
        assert!(matches!(
            stale_captured_read_index,
            ControlPlaneError::ControlPlaneReadIndexNotApplied { .. }
        ));

        let runtime_map = state_machine
            .runtime_map_for_current_applied_read_index(12_346)
            .unwrap();
        assert_eq!(
            runtime_map.freshness_proof(),
            &RuntimeMapFreshnessProof::ReadIndex {
                authority_incarnation: runtime_map.freshness_proof().authority_incarnation(),
                read_index: ControlPlaneLogId::new(2, 2).unwrap(),
                issued_at_ms: 12_346,
            }
        );
    }

    #[test]
    fn control_plane_raft_state_machine_runtime_map_rejects_unapplied_read_index() {
        let mut state_machine = ControlPlaneRaftStateMachine::empty();
        state_machine.apply_entry(blank_entry(3, 7, 1)).unwrap();

        let future_index = state_machine
            .runtime_map_for_applied_read_index(raft_log_id(3, 7, 2), 12_345)
            .unwrap_err();
        assert!(matches!(
            future_index,
            ControlPlaneError::ControlPlaneReadIndexNotApplied { .. }
        ));

        let lower_term_higher_index = state_machine
            .runtime_map_for_applied_read_index(raft_log_id(2, 7, 2), 12_345)
            .unwrap_err();
        assert!(matches!(
            lower_term_higher_index,
            ControlPlaneError::ControlPlaneReadIndexNotApplied { .. }
        ));

        let same_position_different_leader = state_machine
            .runtime_map_for_applied_read_index(raft_log_id(3, 8, 1), 12_345)
            .unwrap_err();
        assert!(matches!(
            same_position_different_leader,
            ControlPlaneError::CommandDecode { .. }
        ));

        let invalid_bootstrap_read_index = state_machine
            .runtime_map_for_applied_read_index(raft_log_id(0, 7, 0), 12_345)
            .unwrap_err();
        assert!(matches!(
            invalid_bootstrap_read_index,
            ControlPlaneError::CommandDecode { .. }
        ));
    }

    #[test]
    fn control_plane_raft_state_machine_trait_apply_drains_entry_responder_stream() {
        let mut state_machine = ControlPlaneRaftStateMachine::empty();
        let entries = stream::iter(vec![
            Ok((blank_entry(1, 1, 1), None)),
            Ok((
                normal_entry(
                    1,
                    1,
                    2,
                    ControlPlaneCommand::BootstrapInitialClusterMap {
                        nodes: vec![(NodeId::new(1), "node-1".to_string())],
                        pg_ids: vec![PgId::new(0)],
                    },
                ),
                None,
            )),
            Ok((
                normal_entry(
                    1,
                    1,
                    3,
                    ControlPlaneCommand::MarkNodeAvailability {
                        node_id: NodeId::new(99),
                        availability: NodeAvailabilityState::Healthy,
                    },
                ),
                None,
            )),
        ]);

        ControlPlaneRaftTypeConfig::run(RaftStateMachine::apply(&mut state_machine, entries))
            .unwrap();

        assert_eq!(state_machine.last_applied(), Some(raft_log_id(1, 1, 3)));
        assert_eq!(
            state_machine
                .inner()
                .last_applied()
                .map(|log_id| (log_id.term(), log_id.index())),
            Some((1, 3))
        );
    }

    #[test]
    fn control_plane_raft_log_store_tracks_vote_committed_and_visible_entries() {
        ControlPlaneRaftTypeConfig::run(async {
            let mut store = ControlPlaneRaftLogStore::empty();
            let vote = Vote::<ControlPlaneRaftLeaderId>::new_committed(3, 1);

            RaftLogStorage::save_vote(&mut store, &vote).await.unwrap();
            assert_eq!(
                RaftLogReader::read_vote(&mut store).await.unwrap(),
                Some(vote)
            );

            RaftLogStorage::append(
                &mut store,
                vec![
                    bootstrap_membership_entry(1),
                    blank_entry(3, 1, 1),
                    blank_entry(3, 1, 2),
                ],
                IOFlushed::noop(),
            )
            .await
            .unwrap();

            let mut reader = RaftLogStorage::get_log_reader(&mut store).await;
            RaftLogStorage::append(&mut store, vec![blank_entry(3, 1, 3)], IOFlushed::noop())
                .await
                .unwrap();

            let entries = RaftLogReader::try_get_log_entries(&mut reader, 0..4)
                .await
                .unwrap();
            assert_eq!(
                entries.iter().map(|entry| entry.log_id).collect::<Vec<_>>(),
                vec![
                    raft_log_id(0, 1, 0),
                    raft_log_id(3, 1, 1),
                    raft_log_id(3, 1, 2),
                    raft_log_id(3, 1, 3),
                ]
            );

            RaftLogStorage::save_committed(&mut store, Some(raft_log_id(3, 1, 2)))
                .await
                .unwrap();
            assert_eq!(
                RaftLogStorage::read_committed(&mut store).await.unwrap(),
                Some(raft_log_id(3, 1, 2))
            );

            let log_state = RaftLogStorage::get_log_state(&mut store).await.unwrap();
            assert_eq!(log_state.last_purged_log_id, None);
            assert_eq!(log_state.last_log_id, Some(raft_log_id(3, 1, 3)));
        });
    }

    #[test]
    fn control_plane_raft_log_store_rejects_vote_regression() {
        ControlPlaneRaftTypeConfig::run(async {
            let mut store = ControlPlaneRaftLogStore::empty();
            let vote = Vote::<ControlPlaneRaftLeaderId>::new(3, 2);

            RaftLogStorage::save_vote(&mut store, &vote).await.unwrap();

            let lower_term = Vote::<ControlPlaneRaftLeaderId>::new(2, 99);
            let err = RaftLogStorage::save_vote(&mut store, &lower_term)
                .await
                .unwrap_err();
            assert!(err.to_string().contains("regress"));
            assert_eq!(
                RaftLogReader::read_vote(&mut store).await.unwrap(),
                Some(vote)
            );

            let lower_node_same_term = Vote::<ControlPlaneRaftLeaderId>::new(3, 1);
            let err = RaftLogStorage::save_vote(&mut store, &lower_node_same_term)
                .await
                .unwrap_err();
            assert!(err.to_string().contains("regress"));

            let committed = Vote::<ControlPlaneRaftLeaderId>::new_committed(3, 2);
            RaftLogStorage::save_vote(&mut store, &committed)
                .await
                .unwrap();

            let uncommitted_same_leader = Vote::<ControlPlaneRaftLeaderId>::new(3, 2);
            let err = RaftLogStorage::save_vote(&mut store, &uncommitted_same_leader)
                .await
                .unwrap_err();
            assert!(err.to_string().contains("regress"));
            assert_eq!(
                RaftLogReader::read_vote(&mut store).await.unwrap(),
                Some(committed)
            );

            let higher = Vote::<ControlPlaneRaftLeaderId>::new(4, 1);
            RaftLogStorage::save_vote(&mut store, &higher)
                .await
                .unwrap();
            assert_eq!(
                RaftLogReader::read_vote(&mut store).await.unwrap(),
                Some(higher)
            );
        });
    }

    #[test]
    fn control_plane_raft_log_store_rejects_invalid_committed_watermarks() {
        ControlPlaneRaftTypeConfig::run(async {
            let mut store = ControlPlaneRaftLogStore::empty();

            let err = RaftLogStorage::save_committed(&mut store, Some(raft_log_id(3, 1, 1)))
                .await
                .unwrap_err();
            assert!(err.to_string().contains("log is empty"));
            assert_eq!(
                RaftLogStorage::read_committed(&mut store).await.unwrap(),
                None
            );

            RaftLogStorage::append(
                &mut store,
                vec![
                    bootstrap_membership_entry(1),
                    blank_entry(3, 1, 1),
                    blank_entry(3, 1, 2),
                ],
                IOFlushed::noop(),
            )
            .await
            .unwrap();
            let err = RaftLogStorage::save_committed(&mut store, Some(raft_log_id(3, 1, 2)))
                .await
                .unwrap_err();
            assert!(err.to_string().contains("missing vote state"));

            let lower_node_same_term_vote = Vote::<ControlPlaneRaftLeaderId>::new_committed(3, 0);
            RaftLogStorage::save_vote(&mut store, &lower_node_same_term_vote)
                .await
                .unwrap();
            let err = RaftLogStorage::save_committed(&mut store, Some(raft_log_id(3, 1, 2)))
                .await
                .unwrap_err();
            assert!(err.to_string().contains("does not cover"));

            let vote = Vote::<ControlPlaneRaftLeaderId>::new_committed(3, 1);
            RaftLogStorage::save_vote(&mut store, &vote).await.unwrap();
            RaftLogStorage::save_committed(&mut store, Some(raft_log_id(3, 1, 2)))
                .await
                .unwrap();

            let err = RaftLogStorage::save_committed(&mut store, Some(raft_log_id(3, 1, 1)))
                .await
                .unwrap_err();
            assert!(err.to_string().contains("regress"));

            let err = RaftLogStorage::save_committed(&mut store, Some(raft_log_id(4, 1, 2)))
                .await
                .unwrap_err();
            assert!(err.to_string().contains("cannot change"));

            let err = RaftLogStorage::save_committed(&mut store, Some(raft_log_id(3, 1, 3)))
                .await
                .unwrap_err();
            assert!(err.to_string().contains("current last log id"));

            let err = RaftLogStorage::save_committed(&mut store, None)
                .await
                .unwrap_err();
            assert!(err.to_string().contains("cannot clear"));

            assert_eq!(
                RaftLogStorage::read_committed(&mut store).await.unwrap(),
                Some(raft_log_id(3, 1, 2))
            );
        });
    }

    #[test]
    fn control_plane_raft_log_store_rejects_truncating_committed_entries() {
        ControlPlaneRaftTypeConfig::run(async {
            let mut store = ControlPlaneRaftLogStore::empty();
            RaftLogStorage::append(
                &mut store,
                vec![
                    bootstrap_membership_entry(1),
                    blank_entry(3, 1, 1),
                    blank_entry(3, 1, 2),
                    blank_entry(3, 1, 3),
                ],
                IOFlushed::noop(),
            )
            .await
            .unwrap();
            let vote = Vote::<ControlPlaneRaftLeaderId>::new_committed(3, 1);
            RaftLogStorage::save_vote(&mut store, &vote).await.unwrap();
            RaftLogStorage::save_committed(&mut store, Some(raft_log_id(3, 1, 2)))
                .await
                .unwrap();

            let err = RaftLogStorage::truncate_after(&mut store, Some(raft_log_id(3, 1, 1)))
                .await
                .unwrap_err();
            assert!(err.to_string().contains("committed log id"));

            let err = RaftLogStorage::truncate_after(&mut store, None)
                .await
                .unwrap_err();
            assert!(err.to_string().contains("committed log id"));

            RaftLogStorage::truncate_after(&mut store, Some(raft_log_id(3, 1, 2)))
                .await
                .unwrap();
            let log_state = RaftLogStorage::get_log_state(&mut store).await.unwrap();
            assert_eq!(log_state.last_log_id, Some(raft_log_id(3, 1, 2)));
            assert_eq!(
                RaftLogStorage::read_committed(&mut store).await.unwrap(),
                Some(raft_log_id(3, 1, 2))
            );
        });
    }

    #[test]
    fn control_plane_raft_log_store_rejects_unknown_truncate_boundaries() {
        ControlPlaneRaftTypeConfig::run(async {
            let mut store = ControlPlaneRaftLogStore::empty();
            RaftLogStorage::append(
                &mut store,
                vec![bootstrap_membership_entry(1), blank_entry(3, 1, 1)],
                IOFlushed::noop(),
            )
            .await
            .unwrap();

            let err = RaftLogStorage::truncate_after(&mut store, Some(raft_log_id(3, 1, 2)))
                .await
                .unwrap_err();
            assert!(err.to_string().contains("current last log id"));

            let err = RaftLogStorage::truncate_after(&mut store, Some(raft_log_id(4, 1, 1)))
                .await
                .unwrap_err();
            assert!(err.to_string().contains("mismatched log id"));

            let vote = Vote::<ControlPlaneRaftLeaderId>::new_committed(3, 1);
            RaftLogStorage::save_vote(&mut store, &vote).await.unwrap();
            RaftLogStorage::save_committed(&mut store, Some(raft_log_id(3, 1, 1)))
                .await
                .unwrap();
            RaftLogStorage::purge(&mut store, raft_log_id(3, 1, 1))
                .await
                .unwrap();
            let err = RaftLogStorage::truncate_after(&mut store, Some(raft_log_id(4, 1, 1)))
                .await
                .unwrap_err();
            assert!(err.to_string().contains("mismatched purged log id"));

            RaftLogStorage::truncate_after(&mut store, Some(raft_log_id(3, 1, 1)))
                .await
                .unwrap();
            let log_state = RaftLogStorage::get_log_state(&mut store).await.unwrap();
            assert_eq!(log_state.last_log_id, Some(raft_log_id(3, 1, 1)));
        });
    }

    #[test]
    fn control_plane_raft_log_store_promotes_committed_gate_when_snapshot_purge_passes_it() {
        ControlPlaneRaftTypeConfig::run(async {
            let mut store = ControlPlaneRaftLogStore::empty();
            RaftLogStorage::append(
                &mut store,
                vec![
                    bootstrap_membership_entry(1),
                    blank_entry(3, 1, 1),
                    blank_entry(3, 1, 2),
                    blank_entry(3, 1, 3),
                ],
                IOFlushed::noop(),
            )
            .await
            .unwrap();
            let vote = Vote::<ControlPlaneRaftLeaderId>::new_committed(3, 1);
            RaftLogStorage::save_vote(&mut store, &vote).await.unwrap();
            RaftLogStorage::save_committed(&mut store, Some(raft_log_id(3, 1, 2)))
                .await
                .unwrap();

            RaftLogStorage::purge(&mut store, raft_log_id(3, 1, 3))
                .await
                .unwrap();
            let log_state = RaftLogStorage::get_log_state(&mut store).await.unwrap();
            assert_eq!(log_state.last_purged_log_id, Some(raft_log_id(3, 1, 3)));
            assert_eq!(
                RaftLogStorage::read_committed(&mut store).await.unwrap(),
                Some(raft_log_id(3, 1, 3))
            );
            let entries = RaftLogReader::try_get_log_entries(&mut store, 0..4)
                .await
                .unwrap();
            assert!(entries.is_empty());
        });
    }

    #[test]
    fn control_plane_openraft_single_node_initialize_uses_bootstrap_membership() {
        ControlPlaneRaftTypeConfig::run(async {
            let mut log_store = ControlPlaneRaftLogStore::empty();
            let state_machine = ControlPlaneRaftStateMachine::empty();
            let raft = Raft::<ControlPlaneRaftTypeConfig, ControlPlaneRaftStateMachine>::new(
                1,
                test_raft_config("control-plane-raft-bootstrap-membership-test"),
                UnreachableRaftNetworkFactory,
                log_store.clone(),
                state_machine,
            )
            .await
            .unwrap();

            let authority = ControlPlaneRaftAuthority::new_with_log_store(raft, log_store.clone());
            authority
                .initialize_membership(BTreeMap::from([(1, BasicNode::new("node-1"))]))
                .await
                .unwrap();
            assert!(authority.is_initialized().await.unwrap());

            let bootstrap_log_id = raft_log_id(0, 1, 0);
            let status = authority.status().await.unwrap();
            assert_eq!(status.effective_membership_log_id(), Some(bootstrap_log_id));
            assert_eq!(status.effective_voters(), &BTreeSet::from([1]));

            let entries = RaftLogReader::try_get_log_entries(&mut log_store, 0..1)
                .await
                .unwrap();
            assert_eq!(entries.len(), 1);
            assert_eq!(entries[0].log_id, bootstrap_log_id);
            assert!(matches!(entries[0].payload, EntryPayload::Membership(_)));

            authority.shutdown().await.unwrap();
        });
    }

    #[test]
    fn control_plane_openraft_triggered_single_node_client_write_applies_and_rejects() {
        ControlPlaneRaftTypeConfig::run(async {
            let log_store = ControlPlaneRaftLogStore::empty();
            let state_machine = ControlPlaneRaftStateMachine::empty();
            let raft = Raft::<ControlPlaneRaftTypeConfig, ControlPlaneRaftStateMachine>::new(
                1,
                test_raft_config("control-plane-raft-client-write-test"),
                UnreachableRaftNetworkFactory,
                log_store.clone(),
                state_machine,
            )
            .await
            .unwrap();

            let authority = ControlPlaneRaftAuthority::new_with_log_store(raft, log_store);
            authority
                .initialize_membership(BTreeMap::from([(1, BasicNode::new("node-1"))]))
                .await
                .unwrap();
            wait_for_local_leader(authority.raft(), "single-node initialization leadership").await;

            let bootstrap = authority
                .submit_control_plane_command(ControlPlaneCommand::BootstrapInitialClusterMap {
                    nodes: vec![(NodeId::new(1), "node-1".to_string())],
                    pg_ids: vec![PgId::new(0)],
                })
                .await
                .unwrap();
            let bootstrap_log_id = bootstrap.log_id();
            assert!(matches!(
                bootstrap.outcome(),
                ControlPlaneRaftCommandOutcome::Applied(
                    ControlPlaneCommandResponse::BootstrapInitialClusterMap
                )
            ));

            let rejected = authority
                .submit_control_plane_command(ControlPlaneCommand::MarkNodeAvailability {
                    node_id: NodeId::new(99),
                    availability: NodeAvailabilityState::Healthy,
                })
                .await
                .unwrap();
            assert_eq!(rejected.log_id().index(), bootstrap_log_id.index() + 1);
            let rejected_log_id = rejected.log_id();
            assert!(matches!(
                rejected.outcome(),
                ControlPlaneRaftCommandOutcome::Rejected(ControlPlaneError::UnknownNode {
                    node_id
                }) if *node_id == 99
            ));

            let (applied_snapshot, (applied_log_id, _applied_membership)) = authority
                .raft()
                .with_state_machine(|state_machine| {
                    let snapshot = state_machine.inner().snapshot().clone();
                    let applied_state = ControlPlaneRaftStateMachine::applied_state(state_machine);
                    Box::pin(async move { (snapshot, applied_state) })
                })
                .await
                .unwrap();
            assert!(applied_snapshot.node(NodeId::new(1)).is_some());
            assert!(applied_snapshot.node(NodeId::new(99)).is_none());
            assert_eq!(applied_log_id, Some(rejected_log_id));

            let status = authority.status().await.unwrap();
            assert_eq!(status.node_id(), 1);
            assert_eq!(status.current_leader(), Some(1));
            let persisted_vote = status
                .persisted_vote()
                .expect("single-node leader should persist a vote");
            assert!(persisted_vote.committed);
            assert_eq!(persisted_vote.leader_id.node_id, 1);
            assert_eq!(
                status.current_term(),
                Some(rejected_log_id.committed_leader_id().term)
            );
            assert_eq!(
                persisted_vote.leader_id.term,
                rejected_log_id.committed_leader_id().term
            );
            assert_eq!(status.last_log_id(), Some(rejected_log_id));
            assert_eq!(status.committed(), Some(rejected_log_id));
            assert_eq!(status.applied(), Some(rejected_log_id));
            assert_eq!(status.effective_voters(), &BTreeSet::from([1]));
            assert_eq!(status.applied_voters(), &BTreeSet::from([1]));

            authority.shutdown().await.unwrap();
        });
    }

    #[test]
    fn control_plane_openraft_read_index_runtime_map_uses_applied_tip() {
        ControlPlaneRaftTypeConfig::run(async {
            let log_store = ControlPlaneRaftLogStore::empty();
            let state_machine = ControlPlaneRaftStateMachine::empty();
            let raft = Raft::<ControlPlaneRaftTypeConfig, ControlPlaneRaftStateMachine>::new(
                1,
                test_raft_config("control-plane-raft-read-index-runtime-map-test"),
                UnreachableRaftNetworkFactory,
                log_store,
                state_machine,
            )
            .await
            .unwrap();

            let authority = ControlPlaneRaftAuthority::new(raft);
            authority
                .initialize_membership(BTreeMap::from([(1, BasicNode::new("node-1"))]))
                .await
                .unwrap();
            wait_for_local_leader(authority.raft(), "single-node read-index leadership").await;

            let write = authority
                .submit_control_plane_command(ControlPlaneCommand::BootstrapInitialClusterMap {
                    nodes: vec![(NodeId::new(1), "node-1".to_string())],
                    pg_ids: vec![PgId::new(0)],
                })
                .await
                .unwrap();
            assert!(matches!(
                write.outcome(),
                ControlPlaneRaftCommandOutcome::Applied(
                    ControlPlaneCommandResponse::BootstrapInitialClusterMap
                )
            ));

            let runtime_map = authority
                .linearized_runtime_map_snapshot(44_000)
                .await
                .unwrap();
            let applied_log_id = authority
                .status()
                .await
                .unwrap()
                .applied()
                .expect("read-index should have an applied tip");
            let expected_control_plane_read_index = control_plane_log_id_from_raft(applied_log_id)
                .expect("read-index should be non-bootstrap");

            assert_eq!(
                runtime_map.freshness_proof().read_index(),
                Some(expected_control_plane_read_index)
            );
            assert_eq!(runtime_map.freshness_proof().issued_at_ms(), Some(44_000));
            assert!(runtime_map.freshness_proof().is_serving_authority_read());
            assert!(runtime_map
                .nodes()
                .iter()
                .any(|node| node.node_id() == NodeId::new(1)));

            authority.shutdown().await.unwrap();
        });
    }

    #[test]
    fn control_plane_openraft_linearized_authority_traits_submit_and_read() {
        ControlPlaneRaftTypeConfig::run(async {
            let log_store = ControlPlaneRaftLogStore::empty();
            let state_machine = ControlPlaneRaftStateMachine::empty();
            let raft = Raft::<ControlPlaneRaftTypeConfig, ControlPlaneRaftStateMachine>::new(
                301,
                test_raft_config("control-plane-raft-linearized-authority-trait-test"),
                UnreachableRaftNetworkFactory,
                log_store,
                state_machine,
            )
            .await
            .unwrap();

            let authority = ControlPlaneRaftAuthority::new(raft);
            authority
                .initialize_membership(BTreeMap::from([(301, BasicNode::new("node-301"))]))
                .await
                .unwrap();
            wait_for_local_leader(authority.raft(), "linearized authority trait leadership").await;

            let command_sink: &dyn ControlPlaneRaftLinearizedCommandSink = &authority;
            let write = command_sink
                .submit_control_plane_command(ControlPlaneCommand::BootstrapInitialClusterMap {
                    nodes: vec![(NodeId::new(301), "node-301".to_string())],
                    pg_ids: vec![PgId::new(0)],
                })
                .await
                .unwrap();
            assert!(matches!(
                write.outcome(),
                ControlPlaneRaftCommandOutcome::Applied(
                    ControlPlaneCommandResponse::BootstrapInitialClusterMap
                )
            ));

            let runtime_map_source: &dyn ControlPlaneRaftLinearizedRuntimeMapSource = &authority;
            let runtime_map = runtime_map_source
                .linearized_runtime_map_snapshot(66_000)
                .await
                .unwrap();
            let expected_read_index = control_plane_log_id_from_raft(
                authority
                    .status()
                    .await
                    .unwrap()
                    .applied()
                    .expect("trait read should have an applied tip"),
            )
            .expect("trait read should be non-bootstrap");

            assert_eq!(
                runtime_map.freshness_proof().read_index(),
                Some(expected_read_index)
            );
            assert_eq!(runtime_map.freshness_proof().issued_at_ms(), Some(66_000));
            assert!(runtime_map
                .nodes()
                .iter()
                .any(|node| node.node_id() == NodeId::new(301)));

            authority.shutdown().await.unwrap();
        });
    }

    #[test]
    fn control_plane_openraft_read_index_requires_leader() {
        ControlPlaneRaftTypeConfig::run(async {
            let log_store = ControlPlaneRaftLogStore::empty();
            let state_machine = ControlPlaneRaftStateMachine::empty();
            let raft = Raft::<ControlPlaneRaftTypeConfig, ControlPlaneRaftStateMachine>::new(
                1,
                test_raft_config("control-plane-raft-read-index-non-leader-test"),
                UnreachableRaftNetworkFactory,
                log_store.clone(),
                state_machine,
            )
            .await
            .unwrap();

            let authority = ControlPlaneRaftAuthority::new(raft);
            authority
                .initialize_membership(BTreeMap::from([
                    (1, BasicNode::new("node-1")),
                    (2, BasicNode::new("node-2")),
                ]))
                .await
                .unwrap();

            let err = authority
                .raft()
                .ensure_linearizable(ReadPolicy::ReadIndex)
                .await
                .unwrap_err();
            let forward_to_leader = err.forward_to_leader().expect("read should need leader");
            assert_eq!(forward_to_leader.leader_id, None);
            assert_eq!(forward_to_leader.leader_node, None);

            let err = authority
                .linearized_runtime_map_snapshot(44_000)
                .await
                .unwrap_err();
            assert!(matches!(
                err,
                ControlPlaneError::RpcRemote { message }
                    if message.contains("OpenRaft read-index failed")
            ));

            let err = authority
                .submit_control_plane_command(ControlPlaneCommand::BootstrapInitialClusterMap {
                    nodes: vec![(NodeId::new(1), "node-1".to_string())],
                    pg_ids: vec![PgId::new(0)],
                })
                .await
                .unwrap_err();
            assert!(matches!(
                err,
                ControlPlaneError::RpcRemote { message }
                    if message.contains("OpenRaft client-write failed")
            ));

            authority.shutdown().await.unwrap();
        });
    }

    #[test]
    fn control_plane_openraft_two_node_client_write_replicates_to_follower() {
        ControlPlaneRaftTypeConfig::run(async {
            let (authority1, authority2) = initialized_two_node_authorities(
                "control-plane-raft-two-node-replication-test",
                101,
                102,
            )
            .await;

            let write = authority1
                .submit_control_plane_command(ControlPlaneCommand::BootstrapInitialClusterMap {
                    nodes: vec![
                        (NodeId::new(101), "node-101".to_string()),
                        (NodeId::new(102), "node-102".to_string()),
                    ],
                    pg_ids: vec![PgId::new(0)],
                })
                .await
                .unwrap();
            assert!(matches!(
                write.outcome(),
                ControlPlaneRaftCommandOutcome::Applied(
                    ControlPlaneCommandResponse::BootstrapInitialClusterMap
                )
            ));

            authority2
                .wait_for_applied_index_at_least(
                    write.log_id().index(),
                    Duration::from_secs(1),
                    "two-node follower applied client write",
                )
                .await
                .unwrap();
            let follower_state = authority2
                .raft()
                .with_state_machine(|state_machine| {
                    let last_applied = state_machine.last_applied();
                    let node_ids = state_machine
                        .inner()
                        .snapshot()
                        .nodes()
                        .map(|node| node.node_id())
                        .collect::<Vec<_>>();
                    Box::pin(async move { (last_applied, node_ids) })
                })
                .await
                .unwrap();
            assert_eq!(follower_state.0, Some(write.log_id()));
            assert_eq!(follower_state.1, vec![NodeId::new(101), NodeId::new(102)]);

            authority1.shutdown().await.unwrap();
            authority2.shutdown().await.unwrap();
        });
    }

    #[test]
    fn control_plane_openraft_two_node_rejected_command_replicates_without_mutation() {
        ControlPlaneRaftTypeConfig::run(async {
            let (authority1, authority2) = initialized_two_node_authorities(
                "control-plane-raft-two-node-rejected-command-test",
                401,
                402,
            )
            .await;

            let bootstrap = authority1
                .submit_control_plane_command(ControlPlaneCommand::BootstrapInitialClusterMap {
                    nodes: vec![
                        (NodeId::new(401), "node-401".to_string()),
                        (NodeId::new(402), "node-402".to_string()),
                    ],
                    pg_ids: vec![PgId::new(0)],
                })
                .await
                .unwrap();
            assert!(matches!(
                bootstrap.outcome(),
                ControlPlaneRaftCommandOutcome::Applied(
                    ControlPlaneCommandResponse::BootstrapInitialClusterMap
                )
            ));

            let rejected = authority1
                .submit_control_plane_command(ControlPlaneCommand::MarkNodeAvailability {
                    node_id: NodeId::new(499),
                    availability: NodeAvailabilityState::Healthy,
                })
                .await
                .unwrap();
            assert_eq!(rejected.log_id().index(), bootstrap.log_id().index() + 1);
            assert!(matches!(
                rejected.outcome(),
                ControlPlaneRaftCommandOutcome::Rejected(ControlPlaneError::UnknownNode {
                    node_id
                }) if *node_id == 499
            ));

            authority2
                .wait_for_applied_index_at_least(
                    rejected.log_id().index(),
                    Duration::from_secs(1),
                    "two-node follower applied rejected command",
                )
                .await
                .unwrap();
            let follower_state = authority2
                .raft()
                .with_state_machine(|state_machine| {
                    let last_applied = state_machine.last_applied();
                    let snapshot = state_machine.inner().snapshot().clone();
                    Box::pin(async move { (last_applied, snapshot) })
                })
                .await
                .unwrap();
            assert_eq!(follower_state.0, Some(rejected.log_id()));
            assert!(follower_state.1.node(NodeId::new(401)).is_some());
            assert!(follower_state.1.node(NodeId::new(402)).is_some());
            assert!(follower_state.1.node(NodeId::new(499)).is_none());

            authority1.shutdown().await.unwrap();
            authority2.shutdown().await.unwrap();
        });
    }

    #[test]
    fn control_plane_openraft_leader_transfer_fences_old_leader() {
        ControlPlaneRaftTypeConfig::run(async {
            let (authority1, authority2) = initialized_two_node_authorities(
                "control-plane-raft-leader-transfer-test",
                701,
                702,
            )
            .await;

            let bootstrap = authority1
                .submit_control_plane_command(ControlPlaneCommand::BootstrapInitialClusterMap {
                    nodes: vec![
                        (NodeId::new(701), "node-701".to_string()),
                        (NodeId::new(702), "node-702".to_string()),
                    ],
                    pg_ids: vec![PgId::new(0)],
                })
                .await
                .unwrap();
            assert!(matches!(
                bootstrap.outcome(),
                ControlPlaneRaftCommandOutcome::Applied(
                    ControlPlaneCommandResponse::BootstrapInitialClusterMap
                )
            ));
            authority2
                .wait_for_applied_index_at_least(
                    bootstrap.log_id().index(),
                    Duration::from_secs(1),
                    "new leader candidate applied bootstrap before transfer",
                )
                .await
                .unwrap();

            authority1.transfer_leadership_to(702).await.unwrap();
            authority1
                .wait_for_current_leader(
                    702,
                    Duration::from_secs(1),
                    "old leader observed transferred leader",
                )
                .await
                .unwrap();
            authority2
                .wait_for_current_leader(
                    702,
                    Duration::from_secs(1),
                    "new leader observed transferred leadership",
                )
                .await
                .unwrap();

            let old_leader_read_err = authority1
                .linearized_runtime_map_snapshot(77_000)
                .await
                .unwrap_err();
            assert!(matches!(
                old_leader_read_err,
                ControlPlaneError::RpcRemote { message }
                    if message.contains("OpenRaft read-index failed")
            ));

            let old_leader_err = authority1
                .submit_control_plane_command(ControlPlaneCommand::MarkNodeAvailability {
                    node_id: NodeId::new(701),
                    availability: NodeAvailabilityState::Unavailable,
                })
                .await
                .unwrap_err();
            assert!(matches!(
                old_leader_err,
                ControlPlaneError::RpcRemote { message }
                    if message.contains("OpenRaft client-write failed")
            ));
            let old_leader_replace_voters_err = authority1
                .replace_voters(BTreeSet::from([701]), false)
                .await
                .unwrap_err();
            assert!(matches!(
                old_leader_replace_voters_err,
                ControlPlaneError::RpcRemote { message }
                    if message.contains("OpenRaft change-membership failed")
            ));
            let old_leader_add_learner_err = authority1
                .add_learner(703, BasicNode::new("node-703"), false)
                .await
                .unwrap_err();
            assert!(matches!(
                old_leader_add_learner_err,
                ControlPlaneError::RpcRemote { message }
                    if message.contains("OpenRaft add-learner failed")
            ));

            let follow_up = authority2
                .submit_control_plane_command(ControlPlaneCommand::MarkNodeAvailability {
                    node_id: NodeId::new(702),
                    availability: NodeAvailabilityState::Unavailable,
                })
                .await
                .unwrap();
            assert!(matches!(
                follow_up.outcome(),
                ControlPlaneRaftCommandOutcome::Applied(
                    ControlPlaneCommandResponse::MarkNodeAvailability
                )
            ));
            assert!(follow_up.log_id().index() > bootstrap.log_id().index());

            authority1
                .wait_for_applied_index_at_least(
                    follow_up.log_id().index(),
                    Duration::from_secs(1),
                    "old leader follower applied post-transfer command",
                )
                .await
                .unwrap();
            let old_status = authority1.status().await.unwrap();
            let new_status = authority2.status().await.unwrap();
            assert_eq!(old_status.current_leader(), Some(702));
            assert_eq!(new_status.current_leader(), Some(702));
            assert_eq!(old_status.applied(), Some(follow_up.log_id()));
            assert_eq!(new_status.applied(), Some(follow_up.log_id()));

            let runtime_map = authority2
                .linearized_runtime_map_snapshot(78_000)
                .await
                .unwrap();
            let expected_read_index = control_plane_log_id_from_raft(follow_up.log_id())
                .expect("post-transfer command log id should be non-bootstrap");
            assert_eq!(
                runtime_map.freshness_proof().read_index(),
                Some(expected_read_index)
            );
            assert_eq!(runtime_map.freshness_proof().issued_at_ms(), Some(78_000));
            assert!(runtime_map.freshness_proof().is_serving_authority_read());

            authority1.shutdown().await.unwrap();
            authority2.shutdown().await.unwrap();
        });
    }

    #[test]
    fn control_plane_openraft_two_node_membership_change_updates_state_machine() {
        ControlPlaneRaftTypeConfig::run(async {
            let (authority1, authority2) = initialized_two_node_authorities(
                "control-plane-raft-two-node-membership-change-test",
                501,
                502,
            )
            .await;

            let bootstrap = authority1
                .submit_control_plane_command(ControlPlaneCommand::BootstrapInitialClusterMap {
                    nodes: vec![
                        (NodeId::new(501), "node-501".to_string()),
                        (NodeId::new(502), "node-502".to_string()),
                    ],
                    pg_ids: vec![PgId::new(0)],
                })
                .await
                .unwrap();
            assert!(matches!(
                bootstrap.outcome(),
                ControlPlaneRaftCommandOutcome::Applied(
                    ControlPlaneCommandResponse::BootstrapInitialClusterMap
                )
            ));
            authority2
                .wait_for_applied_index_at_least(
                    bootstrap.log_id().index(),
                    Duration::from_secs(1),
                    "removed voter candidate applied bootstrap before serving",
                )
                .await
                .unwrap();

            authority1.transfer_leadership_to(502).await.unwrap();
            authority1
                .wait_for_current_leader(
                    502,
                    Duration::from_secs(1),
                    "old leader observed removed voter leadership",
                )
                .await
                .unwrap();
            authority2
                .wait_for_current_leader(
                    502,
                    Duration::from_secs(1),
                    "removed voter became serving leader before removal",
                )
                .await
                .unwrap();

            let pre_removal_runtime_map = authority2
                .linearized_runtime_map_snapshot(50_000)
                .await
                .unwrap();
            assert!(pre_removal_runtime_map
                .freshness_proof()
                .is_serving_authority_read());
            assert_eq!(
                pre_removal_runtime_map.freshness_proof().issued_at_ms(),
                Some(50_000)
            );
            let pre_removal_write = authority2
                .submit_control_plane_command(ControlPlaneCommand::MarkNodeAvailability {
                    node_id: NodeId::new(502),
                    availability: NodeAvailabilityState::Unavailable,
                })
                .await
                .unwrap();
            assert!(matches!(
                pre_removal_write.outcome(),
                ControlPlaneRaftCommandOutcome::Applied(
                    ControlPlaneCommandResponse::MarkNodeAvailability
                )
            ));

            let membership_log_id = authority2
                .replace_voters(BTreeSet::from([501]), false)
                .await
                .unwrap();
            authority1
                .wait_for_applied_index_at_least(
                    membership_log_id.index(),
                    Duration::from_secs(1),
                    "two-node leader applied membership change",
                )
                .await
                .unwrap();
            authority1.raft().trigger().elect(false).await.unwrap();
            authority1
                .wait_for_current_leader(
                    501,
                    Duration::from_secs(1),
                    "remaining voter became leader after membership removal",
                )
                .await
                .unwrap();
            let status = authority1.status().await.unwrap();
            assert_eq!(status.current_leader(), Some(501));
            assert_eq!(
                status.effective_membership_log_id(),
                Some(membership_log_id)
            );
            assert_eq!(status.effective_voters(), &BTreeSet::from([501]));
            assert_eq!(status.applied_membership_log_id(), Some(membership_log_id));
            assert_eq!(status.applied_voters(), &BTreeSet::from([501]));

            let removed_read_err = authority2
                .linearized_runtime_map_snapshot(50_100)
                .await
                .unwrap_err();
            assert!(matches!(
                removed_read_err,
                ControlPlaneError::RpcRemote { message }
                    if message.contains("OpenRaft read-index failed")
            ));
            let removed_write_err = authority2
                .submit_control_plane_command(ControlPlaneCommand::MarkNodeAvailability {
                    node_id: NodeId::new(502),
                    availability: NodeAvailabilityState::Unavailable,
                })
                .await
                .unwrap_err();
            assert!(matches!(
                removed_write_err,
                ControlPlaneError::RpcRemote { message }
                    if message.contains("OpenRaft client-write failed")
            ));

            let follow_up = authority1
                .submit_control_plane_command(ControlPlaneCommand::MarkNodeAvailability {
                    node_id: NodeId::new(501),
                    availability: NodeAvailabilityState::Unavailable,
                })
                .await
                .unwrap();
            assert!(matches!(
                follow_up.outcome(),
                ControlPlaneRaftCommandOutcome::Applied(
                    ControlPlaneCommandResponse::MarkNodeAvailability
                )
            ));
            assert!(follow_up.log_id().index() > membership_log_id.index());
            let follow_up_status = authority1.status().await.unwrap();
            assert_eq!(follow_up_status.applied(), Some(follow_up.log_id()));
            assert_eq!(
                follow_up_status.effective_membership_log_id(),
                Some(membership_log_id)
            );
            assert_eq!(follow_up_status.effective_voters(), &BTreeSet::from([501]));
            assert_eq!(
                follow_up_status.applied_membership_log_id(),
                Some(membership_log_id)
            );
            assert_eq!(follow_up_status.applied_voters(), &BTreeSet::from([501]));

            authority1.shutdown().await.unwrap();
            authority2.shutdown().await.unwrap();
        });
    }

    #[test]
    fn control_plane_openraft_adds_learner_then_promotes_to_voter() {
        ControlPlaneRaftTypeConfig::run(async {
            let (authority1, authority2, authority3) =
                initialized_three_node_cluster_with_two_voters(
                    "control-plane-raft-add-learner-promote-test",
                    601,
                    602,
                    603,
                )
                .await;

            let bootstrap = authority1
                .submit_control_plane_command(ControlPlaneCommand::BootstrapInitialClusterMap {
                    nodes: vec![
                        (NodeId::new(601), "node-601".to_string()),
                        (NodeId::new(602), "node-602".to_string()),
                        (NodeId::new(603), "node-603".to_string()),
                    ],
                    pg_ids: vec![PgId::new(0)],
                })
                .await
                .unwrap();
            assert!(matches!(
                bootstrap.outcome(),
                ControlPlaneRaftCommandOutcome::Applied(
                    ControlPlaneCommandResponse::BootstrapInitialClusterMap
                )
            ));

            let learner_log_id = authority1
                .add_learner(603, BasicNode::new("node-603"), false)
                .await
                .unwrap();
            authority3
                .wait_for_applied_index_at_least(
                    learner_log_id.index(),
                    Duration::from_secs(1),
                    "new learner applied learner membership",
                )
                .await
                .unwrap();
            let learner_status = authority3.status().await.unwrap();
            assert_eq!(learner_status.applied(), Some(learner_log_id));
            assert_eq!(
                learner_status.effective_membership_log_id(),
                Some(learner_log_id)
            );
            assert_eq!(
                learner_status.effective_voters(),
                &BTreeSet::from([601, 602])
            );
            assert_eq!(learner_status.effective_learners(), &BTreeSet::from([603]));
            assert_eq!(
                learner_status.applied_membership_log_id(),
                Some(learner_log_id)
            );
            assert_eq!(learner_status.applied_voters(), &BTreeSet::from([601, 602]));
            assert_eq!(learner_status.applied_learners(), &BTreeSet::from([603]));

            let promote_log_id = authority1
                .replace_voters(BTreeSet::from([601, 602, 603]), true)
                .await
                .unwrap();
            authority1
                .wait_for_applied_index_at_least(
                    promote_log_id.index(),
                    Duration::from_secs(1),
                    "leader applied learner promotion",
                )
                .await
                .unwrap();
            authority3
                .wait_for_applied_index_at_least(
                    promote_log_id.index(),
                    Duration::from_secs(1),
                    "promoted learner applied voter membership",
                )
                .await
                .unwrap();

            let leader_status = authority1.status().await.unwrap();
            assert_eq!(
                leader_status.effective_membership_log_id(),
                Some(promote_log_id)
            );
            assert_eq!(
                leader_status.effective_voters(),
                &BTreeSet::from([601, 602, 603])
            );
            assert_eq!(leader_status.effective_learners(), &BTreeSet::new());
            assert_eq!(
                leader_status.applied_membership_log_id(),
                Some(promote_log_id)
            );
            assert_eq!(
                leader_status.applied_voters(),
                &BTreeSet::from([601, 602, 603])
            );
            assert_eq!(leader_status.applied_learners(), &BTreeSet::new());

            let promoted_status = authority3.status().await.unwrap();
            assert_eq!(promoted_status.applied(), Some(promote_log_id));
            assert_eq!(
                promoted_status.effective_membership_log_id(),
                Some(promote_log_id)
            );
            assert_eq!(
                promoted_status.effective_voters(),
                &BTreeSet::from([601, 602, 603])
            );
            assert_eq!(promoted_status.effective_learners(), &BTreeSet::new());
            assert_eq!(
                promoted_status.applied_membership_log_id(),
                Some(promote_log_id)
            );
            assert_eq!(
                promoted_status.applied_voters(),
                &BTreeSet::from([601, 602, 603])
            );
            assert_eq!(promoted_status.applied_learners(), &BTreeSet::new());

            authority1.shutdown().await.unwrap();
            authority2.shutdown().await.unwrap();
            authority3.shutdown().await.unwrap();
        });
    }

    #[test]
    fn control_plane_openraft_promoted_voter_restart_preserves_membership() {
        ControlPlaneRaftTypeConfig::run(async {
            let network = InMemoryRaftNetworkFactory::default();
            let config = test_raft_config("control-plane-raft-promoted-voter-restart-test");
            let log_store3 = ControlPlaneRaftLogStore::empty();
            let raft1 = Raft::<ControlPlaneRaftTypeConfig, ControlPlaneRaftStateMachine>::new(
                621,
                config.clone(),
                network.clone(),
                ControlPlaneRaftLogStore::empty(),
                ControlPlaneRaftStateMachine::empty(),
            )
            .await
            .unwrap();
            let raft2 = Raft::<ControlPlaneRaftTypeConfig, ControlPlaneRaftStateMachine>::new(
                622,
                config.clone(),
                network.clone(),
                ControlPlaneRaftLogStore::empty(),
                ControlPlaneRaftStateMachine::empty(),
            )
            .await
            .unwrap();
            let raft3 = Raft::<ControlPlaneRaftTypeConfig, ControlPlaneRaftStateMachine>::new(
                623,
                config.clone(),
                network.clone(),
                log_store3.clone(),
                ControlPlaneRaftStateMachine::empty(),
            )
            .await
            .unwrap();
            network.register(621, raft1.clone());
            network.register(622, raft2.clone());
            network.register(623, raft3.clone());
            let authority1 = ControlPlaneRaftAuthority::new(raft1);
            let authority2 = ControlPlaneRaftAuthority::new(raft2);
            let authority3 =
                ControlPlaneRaftAuthority::new_with_log_store(raft3, log_store3.clone());

            authority1
                .initialize_membership(BTreeMap::from([
                    (621, BasicNode::new("node-621")),
                    (622, BasicNode::new("node-622")),
                ]))
                .await
                .unwrap();
            wait_for_local_leader(
                authority1.raft(),
                "two-voter cluster initialized before promoted-voter restart",
            )
            .await;

            let bootstrap = authority1
                .submit_control_plane_command(ControlPlaneCommand::BootstrapInitialClusterMap {
                    nodes: vec![
                        (NodeId::new(621), "node-621".to_string()),
                        (NodeId::new(622), "node-622".to_string()),
                        (NodeId::new(623), "node-623".to_string()),
                    ],
                    pg_ids: vec![PgId::new(0)],
                })
                .await
                .unwrap();
            assert!(matches!(
                bootstrap.outcome(),
                ControlPlaneRaftCommandOutcome::Applied(
                    ControlPlaneCommandResponse::BootstrapInitialClusterMap
                )
            ));

            let learner_log_id = authority1
                .add_learner(623, BasicNode::new("node-623"), false)
                .await
                .unwrap();
            authority3
                .wait_for_applied_index_at_least(
                    learner_log_id.index(),
                    Duration::from_secs(1),
                    "restart candidate applied learner membership",
                )
                .await
                .unwrap();

            let promote_log_id = authority1
                .replace_voters(BTreeSet::from([621, 622, 623]), true)
                .await
                .unwrap();
            authority3
                .wait_for_applied_index_at_least(
                    promote_log_id.index(),
                    Duration::from_secs(1),
                    "restart candidate applied voter promotion",
                )
                .await
                .unwrap();
            let pre_restart_status = authority3.status().await.unwrap();
            assert_eq!(pre_restart_status.applied(), Some(promote_log_id));
            assert_eq!(
                pre_restart_status.effective_membership_log_id(),
                Some(promote_log_id)
            );
            assert_eq!(
                pre_restart_status.effective_voters(),
                &BTreeSet::from([621, 622, 623])
            );
            assert_eq!(
                pre_restart_status.applied_membership_log_id(),
                Some(promote_log_id)
            );
            assert_eq!(
                pre_restart_status.applied_voters(),
                &BTreeSet::from([621, 622, 623])
            );

            let restart_artifact =
                capture_openraft_restart_artifact(&log_store3, &authority3).await;
            authority3.shutdown().await.unwrap();
            network.unregister(623);

            let (restored_log_store, restored_state_machine) = restart_artifact.restore().unwrap();
            let restored_log_store_for_status = restored_log_store.clone();
            let restarted_raft =
                Raft::<ControlPlaneRaftTypeConfig, ControlPlaneRaftStateMachine>::new(
                    623,
                    config,
                    network.clone(),
                    restored_log_store,
                    restored_state_machine,
                )
                .await
                .unwrap();
            network.register(623, restarted_raft.clone());
            let restarted_authority = ControlPlaneRaftAuthority::new_with_log_store(
                restarted_raft,
                restored_log_store_for_status,
            );

            let restarted_status = restarted_authority.status().await.unwrap();
            assert_eq!(restarted_status.applied(), Some(promote_log_id));
            assert_eq!(
                restarted_status.effective_membership_log_id(),
                Some(promote_log_id)
            );
            assert_eq!(
                restarted_status.effective_voters(),
                &BTreeSet::from([621, 622, 623])
            );
            assert_eq!(
                restarted_status.applied_membership_log_id(),
                Some(promote_log_id)
            );
            assert_eq!(
                restarted_status.applied_voters(),
                &BTreeSet::from([621, 622, 623])
            );

            let post_restart_write = authority1
                .submit_control_plane_command(ControlPlaneCommand::MarkNodeAvailability {
                    node_id: NodeId::new(623),
                    availability: NodeAvailabilityState::Unavailable,
                })
                .await
                .unwrap();
            assert!(matches!(
                post_restart_write.outcome(),
                ControlPlaneRaftCommandOutcome::Applied(
                    ControlPlaneCommandResponse::MarkNodeAvailability
                )
            ));
            assert!(post_restart_write.log_id().index() > promote_log_id.index());
            restarted_authority
                .wait_for_applied_index_at_least(
                    post_restart_write.log_id().index(),
                    Duration::from_secs(1),
                    "restarted promoted voter applied post-restart command",
                )
                .await
                .unwrap();
            let caught_up_status = restarted_authority.status().await.unwrap();
            assert_eq!(
                caught_up_status.applied(),
                Some(post_restart_write.log_id())
            );
            assert_eq!(
                caught_up_status.effective_membership_log_id(),
                Some(promote_log_id)
            );
            assert_eq!(
                caught_up_status.effective_voters(),
                &BTreeSet::from([621, 622, 623])
            );
            assert_eq!(
                caught_up_status.applied_membership_log_id(),
                Some(promote_log_id)
            );
            assert_eq!(
                caught_up_status.applied_voters(),
                &BTreeSet::from([621, 622, 623])
            );
            let node623_availability = restarted_authority
                .raft()
                .with_state_machine(|state_machine| {
                    let availability = state_machine
                        .inner()
                        .snapshot()
                        .node(NodeId::new(623))
                        .map(|node| node.availability());
                    Box::pin(async move { availability })
                })
                .await
                .unwrap();
            assert_eq!(
                node623_availability,
                Some(NodeAvailabilityState::Unavailable)
            );

            authority1.shutdown().await.unwrap();
            authority2.shutdown().await.unwrap();
            restarted_authority.shutdown().await.unwrap();
        });
    }

    #[test]
    fn control_plane_openraft_two_node_read_index_runtime_map_uses_quorum_applied_tip() {
        ControlPlaneRaftTypeConfig::run(async {
            let (authority1, authority2) = initialized_two_node_authorities(
                "control-plane-raft-two-node-read-index-runtime-map-test",
                201,
                202,
            )
            .await;

            let write = authority1
                .submit_control_plane_command(ControlPlaneCommand::BootstrapInitialClusterMap {
                    nodes: vec![
                        (NodeId::new(201), "node-201".to_string()),
                        (NodeId::new(202), "node-202".to_string()),
                    ],
                    pg_ids: vec![PgId::new(0)],
                })
                .await
                .unwrap();
            assert!(matches!(
                write.outcome(),
                ControlPlaneRaftCommandOutcome::Applied(
                    ControlPlaneCommandResponse::BootstrapInitialClusterMap
                )
            ));
            authority2
                .wait_for_applied_index_at_least(
                    write.log_id().index(),
                    Duration::from_secs(1),
                    "two-node follower applied before read-index",
                )
                .await
                .unwrap();

            let runtime_map = authority1
                .linearized_runtime_map_snapshot(55_000)
                .await
                .unwrap();
            let applied_log_id = authority1
                .status()
                .await
                .unwrap()
                .applied()
                .expect("read-index should have an applied tip");
            assert!(applied_log_id.index() >= write.log_id().index());
            let expected_read_index = control_plane_log_id_from_raft(applied_log_id)
                .expect("read-index should be non-bootstrap");

            assert_eq!(
                runtime_map.freshness_proof().read_index(),
                Some(expected_read_index)
            );
            assert_eq!(runtime_map.freshness_proof().issued_at_ms(), Some(55_000));
            assert!(runtime_map.freshness_proof().is_serving_authority_read());
            assert!(runtime_map
                .nodes()
                .iter()
                .any(|node| node.node_id() == NodeId::new(201)));
            assert!(runtime_map
                .nodes()
                .iter()
                .any(|node| node.node_id() == NodeId::new(202)));

            authority1.shutdown().await.unwrap();
            authority2.shutdown().await.unwrap();
        });
    }

    #[test]
    fn control_plane_openraft_restarted_follower_catches_up_committed_prefix() {
        ControlPlaneRaftTypeConfig::run(async {
            let ThreeVoterAuthorityFixture {
                network,
                config,
                leader_log_store: _,
                third_log_store,
                authority1,
                authority2,
                authority3,
            } = initialized_three_node_voter_authorities(
                "control-plane-raft-follower-restart-catch-up-test",
                801,
                802,
                803,
            )
            .await;

            let bootstrap = authority1
                .submit_control_plane_command(ControlPlaneCommand::BootstrapInitialClusterMap {
                    nodes: vec![
                        (NodeId::new(801), "node-801".to_string()),
                        (NodeId::new(802), "node-802".to_string()),
                        (NodeId::new(803), "node-803".to_string()),
                    ],
                    pg_ids: vec![PgId::new(0)],
                })
                .await
                .unwrap();
            assert!(matches!(
                bootstrap.outcome(),
                ControlPlaneRaftCommandOutcome::Applied(
                    ControlPlaneCommandResponse::BootstrapInitialClusterMap
                )
            ));
            authority3
                .wait_for_applied_index_at_least(
                    bootstrap.log_id().index(),
                    Duration::from_secs(1),
                    "third voter applied bootstrap before restart",
                )
                .await
                .unwrap();

            let restart_artifact =
                capture_openraft_restart_artifact(&third_log_store, &authority3).await;
            authority3.shutdown().await.unwrap();
            network.unregister(803);

            let offline_write = authority1
                .submit_control_plane_command(ControlPlaneCommand::MarkNodeAvailability {
                    node_id: NodeId::new(802),
                    availability: NodeAvailabilityState::Unavailable,
                })
                .await
                .unwrap();
            assert!(matches!(
                offline_write.outcome(),
                ControlPlaneRaftCommandOutcome::Applied(
                    ControlPlaneCommandResponse::MarkNodeAvailability
                )
            ));
            authority2
                .wait_for_applied_index_at_least(
                    offline_write.log_id().index(),
                    Duration::from_secs(1),
                    "second voter applied command committed while third voter was down",
                )
                .await
                .unwrap();

            let (restored_log_store, restored_state_machine) = restart_artifact.restore().unwrap();
            let restored_log_store_for_status = restored_log_store.clone();
            let restarted_raft =
                Raft::<ControlPlaneRaftTypeConfig, ControlPlaneRaftStateMachine>::new(
                    803,
                    config,
                    network.clone(),
                    restored_log_store,
                    restored_state_machine,
                )
                .await
                .unwrap();
            network.register(803, restarted_raft.clone());
            let restarted_authority = ControlPlaneRaftAuthority::new_with_log_store(
                restarted_raft,
                restored_log_store_for_status,
            );

            let catch_up_trigger = authority1
                .submit_control_plane_command(ControlPlaneCommand::MarkNodeAvailability {
                    node_id: NodeId::new(803),
                    availability: NodeAvailabilityState::Unavailable,
                })
                .await
                .unwrap();
            assert!(matches!(
                catch_up_trigger.outcome(),
                ControlPlaneRaftCommandOutcome::Applied(
                    ControlPlaneCommandResponse::MarkNodeAvailability
                )
            ));
            assert!(catch_up_trigger.log_id().index() > offline_write.log_id().index());

            restarted_authority
                .wait_for_applied_index_at_least(
                    catch_up_trigger.log_id().index(),
                    Duration::from_secs(1),
                    "restarted third voter caught up missing committed prefix",
                )
                .await
                .unwrap();
            let restarted_status = restarted_authority.status().await.unwrap();
            assert_eq!(restarted_status.current_leader(), Some(801));
            assert_eq!(restarted_status.applied(), Some(catch_up_trigger.log_id()));
            assert_eq!(
                restarted_status.effective_voters(),
                &BTreeSet::from([801, 802, 803])
            );

            let restarted_state = restarted_authority
                .raft()
                .with_state_machine(|state_machine| {
                    let snapshot = state_machine.inner().snapshot();
                    let node_ids = snapshot
                        .nodes()
                        .map(|node| node.node_id())
                        .collect::<Vec<_>>();
                    let node802_availability = snapshot
                        .node(NodeId::new(802))
                        .map(|node| node.availability());
                    let node803_availability = snapshot
                        .node(NodeId::new(803))
                        .map(|node| node.availability());
                    Box::pin(async move { (node_ids, node802_availability, node803_availability) })
                })
                .await
                .unwrap();
            assert_eq!(
                restarted_state.0,
                vec![NodeId::new(801), NodeId::new(802), NodeId::new(803)]
            );
            assert_eq!(restarted_state.1, Some(NodeAvailabilityState::Unavailable));
            assert_eq!(restarted_state.2, Some(NodeAvailabilityState::Unavailable));

            authority1.shutdown().await.unwrap();
            authority2.shutdown().await.unwrap();
            restarted_authority.shutdown().await.unwrap();
        });
    }

    #[test]
    fn control_plane_openraft_restarted_follower_catches_up_from_leader_snapshot() {
        ControlPlaneRaftTypeConfig::run(async {
            let ThreeVoterAuthorityFixture {
                network,
                config,
                leader_log_store,
                third_log_store,
                authority1,
                authority2,
                authority3,
            } = initialized_three_node_voter_authorities_with_config(
                test_raft_config_with_log_reversion(
                    "control-plane-raft-follower-snapshot-catch-up-test",
                    Some(true),
                ),
                1101,
                1102,
                1103,
            )
            .await;

            let bootstrap = authority1
                .submit_control_plane_command(ControlPlaneCommand::BootstrapInitialClusterMap {
                    nodes: vec![
                        (NodeId::new(1101), "node-1101".to_string()),
                        (NodeId::new(1102), "node-1102".to_string()),
                        (NodeId::new(1103), "node-1103".to_string()),
                    ],
                    pg_ids: vec![PgId::new(0)],
                })
                .await
                .unwrap();
            assert!(matches!(
                bootstrap.outcome(),
                ControlPlaneRaftCommandOutcome::Applied(
                    ControlPlaneCommandResponse::BootstrapInitialClusterMap
                )
            ));
            authority3
                .wait_for_applied_index_at_least(
                    bootstrap.log_id().index(),
                    Duration::from_secs(1),
                    "third voter applied bootstrap before snapshot catch-up restart",
                )
                .await
                .unwrap();

            let restart_artifact =
                capture_openraft_restart_artifact(&third_log_store, &authority3).await;

            let offline_write = authority1
                .submit_control_plane_command(ControlPlaneCommand::MarkNodeAvailability {
                    node_id: NodeId::new(1102),
                    availability: NodeAvailabilityState::Unavailable,
                })
                .await
                .unwrap();
            assert!(matches!(
                offline_write.outcome(),
                ControlPlaneRaftCommandOutcome::Applied(
                    ControlPlaneCommandResponse::MarkNodeAvailability
                )
            ));
            authority2
                .wait_for_applied_index_at_least(
                    offline_write.log_id().index(),
                    Duration::from_secs(1),
                    "second voter applied command before leader snapshot purge",
                )
                .await
                .unwrap();
            authority3
                .wait_for_applied_index_at_least(
                    offline_write.log_id().index(),
                    Duration::from_secs(1),
                    "third voter applied command before leader snapshot purge",
                )
                .await
                .unwrap();

            let mut snapshot_progress = authority1.raft().watch_snapshot_progress();
            authority1.raft().trigger().snapshot().await.unwrap();
            snapshot_progress
                .wait_until_ge(&Some(offline_write.log_id()))
                .await
                .unwrap();
            let leader_snapshot = authority1.raft().get_snapshot().await.unwrap().unwrap();
            assert_eq!(
                leader_snapshot.meta.last_log_id,
                Some(offline_write.log_id())
            );

            authority1
                .raft()
                .trigger()
                .purge_log(offline_write.log_id().index())
                .await
                .unwrap();
            wait_for_log_purged_to(
                &leader_log_store,
                offline_write.log_id(),
                "leader purged log prefix covered by snapshot",
            )
            .await;
            let leader_status = authority1.status().await.unwrap();
            let leader_vote = leader_status
                .persisted_vote()
                .expect("leader should report its persisted vote");
            assert!(leader_vote.committed);
            assert_eq!(leader_vote.leader_id.node_id, 1101);
            assert_eq!(
                leader_status.current_term(),
                Some(offline_write.log_id().committed_leader_id().term)
            );
            assert_eq!(
                leader_status.last_purged_log_id(),
                Some(offline_write.log_id())
            );

            authority3.shutdown().await.unwrap();
            network.unregister(1103);

            let (restored_log_store, restored_state_machine) = restart_artifact.restore().unwrap();
            let restored_log_store_for_status = restored_log_store.clone();
            let restarted_raft =
                Raft::<ControlPlaneRaftTypeConfig, ControlPlaneRaftStateMachine>::new(
                    1103,
                    config,
                    network.clone(),
                    restored_log_store,
                    restored_state_machine,
                )
                .await
                .unwrap();
            network.register(1103, restarted_raft.clone());
            let restarted_authority = ControlPlaneRaftAuthority::new_with_log_store(
                restarted_raft,
                restored_log_store_for_status,
            );
            authority1
                .raft()
                .trigger()
                .allow_next_revert(&1103, true)
                .await
                .unwrap()
                .unwrap();

            let catch_up_trigger = authority1
                .submit_control_plane_command(ControlPlaneCommand::MarkNodeAvailability {
                    node_id: NodeId::new(1103),
                    availability: NodeAvailabilityState::Unavailable,
                })
                .await
                .unwrap();
            assert!(matches!(
                catch_up_trigger.outcome(),
                ControlPlaneRaftCommandOutcome::Applied(
                    ControlPlaneCommandResponse::MarkNodeAvailability
                )
            ));

            restarted_authority
                .wait_for_applied_index_at_least(
                    catch_up_trigger.log_id().index(),
                    Duration::from_secs(1),
                    "restarted third voter caught up through leader snapshot",
                )
                .await
                .unwrap();
            let restarted_status = restarted_authority.status().await.unwrap();
            assert_eq!(restarted_status.applied(), Some(catch_up_trigger.log_id()));
            let restarted_vote = restarted_status
                .persisted_vote()
                .expect("restarted follower should retain its persisted vote");
            assert_eq!(restarted_vote.leader_id.node_id, 1101);
            assert_eq!(
                restarted_status.current_term(),
                Some(catch_up_trigger.log_id().committed_leader_id().term)
            );
            assert_eq!(
                restarted_status.last_purged_log_id(),
                Some(offline_write.log_id())
            );
            assert_eq!(
                restarted_status.current_snapshot(),
                Some(offline_write.log_id())
            );

            let restarted_state = restarted_authority
                .raft()
                .with_state_machine(|state_machine| {
                    let snapshot_log_id = state_machine
                        .current_snapshot()
                        .and_then(|snapshot| snapshot.meta.last_log_id);
                    let snapshot = state_machine.inner().snapshot();
                    let node1102_availability = snapshot
                        .node(NodeId::new(1102))
                        .map(|node| node.availability());
                    let node1103_availability = snapshot
                        .node(NodeId::new(1103))
                        .map(|node| node.availability());
                    Box::pin(async move {
                        (
                            snapshot_log_id,
                            node1102_availability,
                            node1103_availability,
                        )
                    })
                })
                .await
                .unwrap();
            assert_eq!(restarted_state.0, Some(offline_write.log_id()));
            assert_eq!(restarted_state.1, Some(NodeAvailabilityState::Unavailable));
            assert_eq!(restarted_state.2, Some(NodeAvailabilityState::Unavailable));

            authority1.shutdown().await.unwrap();
            authority2.shutdown().await.unwrap();
            restarted_authority.shutdown().await.unwrap();
        });
    }

    #[test]
    fn control_plane_openraft_restarted_leader_resumes_writes_and_reads() {
        ControlPlaneRaftTypeConfig::run(async {
            let ThreeVoterAuthorityFixture {
                network,
                config,
                leader_log_store,
                third_log_store: _,
                authority1,
                authority2,
                authority3,
            } = initialized_three_node_voter_authorities(
                "control-plane-raft-leader-restart-resume-test",
                901,
                902,
                903,
            )
            .await;

            let bootstrap = authority1
                .submit_control_plane_command(ControlPlaneCommand::BootstrapInitialClusterMap {
                    nodes: vec![
                        (NodeId::new(901), "node-901".to_string()),
                        (NodeId::new(902), "node-902".to_string()),
                        (NodeId::new(903), "node-903".to_string()),
                    ],
                    pg_ids: vec![PgId::new(0)],
                })
                .await
                .unwrap();
            assert!(matches!(
                bootstrap.outcome(),
                ControlPlaneRaftCommandOutcome::Applied(
                    ControlPlaneCommandResponse::BootstrapInitialClusterMap
                )
            ));
            authority2
                .wait_for_applied_index_at_least(
                    bootstrap.log_id().index(),
                    Duration::from_secs(1),
                    "second voter applied bootstrap before leader restart",
                )
                .await
                .unwrap();
            authority3
                .wait_for_applied_index_at_least(
                    bootstrap.log_id().index(),
                    Duration::from_secs(1),
                    "third voter applied bootstrap before leader restart",
                )
                .await
                .unwrap();

            let restart_artifact =
                capture_openraft_restart_artifact(&leader_log_store, &authority1).await;
            authority1.shutdown().await.unwrap();
            network.unregister(901);

            let follower_write_err = authority2
                .submit_control_plane_command(ControlPlaneCommand::MarkNodeAvailability {
                    node_id: NodeId::new(902),
                    availability: NodeAvailabilityState::Unavailable,
                })
                .await
                .unwrap_err();
            assert!(matches!(
                follower_write_err,
                ControlPlaneError::RpcRemote { message }
                    if message.contains("OpenRaft client-write failed")
            ));

            let (restored_log_store, restored_state_machine) = restart_artifact.restore().unwrap();
            let restored_log_store_for_status = restored_log_store.clone();
            let restarted_raft =
                Raft::<ControlPlaneRaftTypeConfig, ControlPlaneRaftStateMachine>::new(
                    901,
                    config,
                    network.clone(),
                    restored_log_store,
                    restored_state_machine,
                )
                .await
                .unwrap();
            network.register(901, restarted_raft.clone());
            let restarted_authority = ControlPlaneRaftAuthority::new_with_log_store(
                restarted_raft,
                restored_log_store_for_status,
            );
            restarted_authority
                .wait_for_current_leader(
                    901,
                    Duration::from_secs(1),
                    "restarted leader recovered current leadership",
                )
                .await
                .unwrap();

            let resumed_write = restarted_authority
                .submit_control_plane_command(ControlPlaneCommand::MarkNodeAvailability {
                    node_id: NodeId::new(903),
                    availability: NodeAvailabilityState::Unavailable,
                })
                .await
                .unwrap();
            assert!(matches!(
                resumed_write.outcome(),
                ControlPlaneRaftCommandOutcome::Applied(
                    ControlPlaneCommandResponse::MarkNodeAvailability
                )
            ));
            assert!(resumed_write.log_id().index() > bootstrap.log_id().index());
            authority2
                .wait_for_applied_index_at_least(
                    resumed_write.log_id().index(),
                    Duration::from_secs(1),
                    "second voter applied restarted leader write",
                )
                .await
                .unwrap();
            authority3
                .wait_for_applied_index_at_least(
                    resumed_write.log_id().index(),
                    Duration::from_secs(1),
                    "third voter applied restarted leader write",
                )
                .await
                .unwrap();

            let runtime_map = restarted_authority
                .linearized_runtime_map_snapshot(90_000)
                .await
                .unwrap();
            let expected_read_index = control_plane_log_id_from_raft(resumed_write.log_id())
                .expect("restarted leader command log id should be non-bootstrap");
            assert_eq!(
                runtime_map.freshness_proof().read_index(),
                Some(expected_read_index)
            );
            assert_eq!(runtime_map.freshness_proof().issued_at_ms(), Some(90_000));
            assert_eq!(
                runtime_map
                    .nodes()
                    .iter()
                    .map(|node| node.node_id())
                    .collect::<Vec<_>>(),
                vec![NodeId::new(901), NodeId::new(902), NodeId::new(903)]
            );
            let restarted_status = restarted_authority.status().await.unwrap();
            assert_eq!(restarted_status.applied(), Some(resumed_write.log_id()));
            assert_eq!(
                restarted_status.authority_incarnation(),
                runtime_map.freshness_proof().authority_incarnation()
            );
            assert_eq!(
                restarted_status.current_cluster_epoch(),
                runtime_map.cluster_epoch()
            );
            assert_eq!(restarted_status.oldest_storage_history_floor_epoch(), None);
            assert!(
                restarted_status.retained_history_count() > 0,
                "restarted leader should retain historical route state after epoch changes"
            );
            assert!(
                restarted_status.oldest_retained_history_epoch()
                    <= restarted_status.newest_retained_history_epoch()
            );
            assert!(restarted_status
                .newest_retained_history_epoch()
                .is_some_and(|epoch| epoch < restarted_status.current_cluster_epoch()));
            assert_eq!(restarted_status.storage_node_count(), 3);
            assert_eq!(restarted_status.joining_storage_node_count(), 0);
            assert_eq!(restarted_status.active_storage_node_count(), 3);
            assert_eq!(restarted_status.draining_storage_node_count(), 0);
            assert_eq!(restarted_status.out_storage_node_count(), 0);
            assert_eq!(restarted_status.removed_storage_node_count(), 0);
            assert_eq!(restarted_status.healthy_storage_node_count(), 0);
            assert_eq!(restarted_status.suspect_storage_node_count(), 2);
            assert_eq!(restarted_status.unavailable_storage_node_count(), 1);
            assert_eq!(restarted_status.pg_count(), 1);
            assert_eq!(restarted_status.active_pg_count(), 0);
            assert_eq!(restarted_status.peering_pg_count(), 1);
            assert_eq!(restarted_status.degraded_pg_count(), 0);
            assert_eq!(restarted_status.backfilling_pg_count(), 0);
            assert_eq!(restarted_status.inconsistent_pg_count(), 0);
            assert_eq!(restarted_status.active_primary_pg_count(), 0);
            assert_eq!(restarted_status.peering_metadata_transfer_pg_count(), 0);
            assert_eq!(restarted_status.metadata_transfer_fenced_pg_count(), 0);

            let restarted_node903_availability = restarted_authority
                .raft()
                .with_state_machine(|state_machine| {
                    let availability = state_machine
                        .inner()
                        .snapshot()
                        .node(NodeId::new(903))
                        .map(|node| node.availability());
                    Box::pin(async move { availability })
                })
                .await
                .unwrap();
            assert_eq!(
                restarted_node903_availability,
                Some(NodeAvailabilityState::Unavailable)
            );

            restarted_authority.shutdown().await.unwrap();
            authority2.shutdown().await.unwrap();
            authority3.shutdown().await.unwrap();
        });
    }

    #[test]
    fn control_plane_openraft_transferred_leader_continues_after_old_leader_loss() {
        ControlPlaneRaftTypeConfig::run(async {
            let ThreeVoterAuthorityFixture {
                network,
                config: _,
                leader_log_store: _,
                third_log_store: _,
                authority1,
                authority2,
                authority3,
            } = initialized_three_node_voter_authorities(
                "control-plane-raft-post-transfer-old-leader-loss-test",
                1001,
                1002,
                1003,
            )
            .await;

            let bootstrap = authority1
                .submit_control_plane_command(ControlPlaneCommand::BootstrapInitialClusterMap {
                    nodes: vec![
                        (NodeId::new(1001), "node-1001".to_string()),
                        (NodeId::new(1002), "node-1002".to_string()),
                        (NodeId::new(1003), "node-1003".to_string()),
                    ],
                    pg_ids: vec![PgId::new(0)],
                })
                .await
                .unwrap();
            assert!(matches!(
                bootstrap.outcome(),
                ControlPlaneRaftCommandOutcome::Applied(
                    ControlPlaneCommandResponse::BootstrapInitialClusterMap
                )
            ));
            authority2
                .wait_for_applied_index_at_least(
                    bootstrap.log_id().index(),
                    Duration::from_secs(1),
                    "second voter applied bootstrap before leader loss",
                )
                .await
                .unwrap();
            authority3
                .wait_for_applied_index_at_least(
                    bootstrap.log_id().index(),
                    Duration::from_secs(1),
                    "third voter applied bootstrap before leader loss",
                )
                .await
                .unwrap();

            authority1.transfer_leadership_to(1002).await.unwrap();
            authority2
                .wait_for_current_leader(
                    1002,
                    Duration::from_secs(1),
                    "second voter accepted leadership before old leader loss",
                )
                .await
                .unwrap();
            authority3
                .wait_for_current_leader(
                    1002,
                    Duration::from_secs(1),
                    "third voter learned transferred leader before old leader loss",
                )
                .await
                .unwrap();

            authority1.shutdown().await.unwrap();
            network.unregister(1001);

            let failover_write = authority2
                .submit_control_plane_command(ControlPlaneCommand::MarkNodeAvailability {
                    node_id: NodeId::new(1001),
                    availability: NodeAvailabilityState::Unavailable,
                })
                .await
                .unwrap();
            assert!(matches!(
                failover_write.outcome(),
                ControlPlaneRaftCommandOutcome::Applied(
                    ControlPlaneCommandResponse::MarkNodeAvailability
                )
            ));
            assert_eq!(failover_write.log_id().committed_leader_id().node_id, 1002);
            assert!(
                failover_write.log_id().committed_leader_id().term
                    > bootstrap.log_id().committed_leader_id().term
            );
            authority3
                .wait_for_applied_index_at_least(
                    failover_write.log_id().index(),
                    Duration::from_secs(1),
                    "third voter applied failover leader write",
                )
                .await
                .unwrap();

            let follower_state = authority3
                .raft()
                .with_state_machine(|state_machine| {
                    let snapshot = state_machine.inner().snapshot();
                    let node_ids = snapshot
                        .nodes()
                        .map(|node| node.node_id())
                        .collect::<Vec<_>>();
                    let node1001_availability = snapshot
                        .node(NodeId::new(1001))
                        .map(|node| node.availability());
                    Box::pin(async move { (node_ids, node1001_availability) })
                })
                .await
                .unwrap();
            assert_eq!(
                follower_state.0,
                vec![NodeId::new(1001), NodeId::new(1002), NodeId::new(1003)]
            );
            assert_eq!(follower_state.1, Some(NodeAvailabilityState::Unavailable));

            authority2.shutdown().await.unwrap();
            authority3.shutdown().await.unwrap();
        });
    }

    #[test]
    fn control_plane_openraft_restart_replays_committed_entries() {
        ControlPlaneRaftTypeConfig::run(async {
            let mut log_store = ControlPlaneRaftLogStore::empty();
            RaftLogStorage::append(
                &mut log_store,
                vec![
                    bootstrap_membership_entry(1),
                    blank_entry(3, 1, 1),
                    membership_entry(3, 1, 2),
                    blank_entry(3, 1, 3),
                ],
                IOFlushed::noop(),
            )
            .await
            .unwrap();
            let vote = Vote::<ControlPlaneRaftLeaderId>::new_committed(3, 1);
            RaftLogStorage::save_vote(&mut log_store, &vote)
                .await
                .unwrap();
            RaftLogStorage::save_committed(&mut log_store, Some(raft_log_id(3, 1, 3)))
                .await
                .unwrap();

            let mut state_machine = ControlPlaneRaftStateMachine::empty();
            state_machine
                .apply_entry(bootstrap_membership_entry(1))
                .unwrap();
            state_machine.apply_entry(blank_entry(3, 1, 1)).unwrap();

            let artifact =
                ControlPlaneRaftRestartArtifact::capture(&log_store, &state_machine).unwrap();
            let (mut restored_log_store, restored_state_machine) = artifact.restore().unwrap();
            assert_eq!(
                restored_state_machine.last_applied(),
                Some(raft_log_id(3, 1, 1))
            );

            let raft = Raft::<ControlPlaneRaftTypeConfig, ControlPlaneRaftStateMachine>::new(
                1,
                test_raft_config("control-plane-raft-restart-replay-test"),
                UnreachableRaftNetworkFactory,
                restored_log_store.clone(),
                restored_state_machine,
            )
            .await
            .unwrap();

            assert!(raft.is_initialized().await.unwrap());
            let raft_state = raft
                .with_raft_state(|state| {
                    (
                        state.log_ids.last().cloned(),
                        state.local_committed().cloned(),
                        *state.membership_state.effective().log_id(),
                    )
                })
                .await
                .unwrap();
            assert_eq!(raft_state.0, Some(raft_log_id(3, 1, 3)));
            assert_eq!(raft_state.1, Some(raft_log_id(3, 1, 3)));
            assert_eq!(raft_state.2, Some(raft_log_id(3, 1, 2)));
            let applied_state = raft
                .with_state_machine(|state_machine| {
                    let applied_state = ControlPlaneRaftStateMachine::applied_state(state_machine);
                    Box::pin(async move { applied_state })
                })
                .await
                .unwrap();
            assert_eq!(applied_state.0, Some(raft_log_id(3, 1, 3)));
            assert_eq!(applied_state.1.log_id(), &Some(raft_log_id(3, 1, 2)));
            assert_eq!(
                RaftLogStorage::read_committed(&mut restored_log_store)
                    .await
                    .unwrap(),
                Some(raft_log_id(3, 1, 3))
            );

            raft.shutdown().await.unwrap();
        });
    }

    #[test]
    fn control_plane_openraft_restart_replays_rejected_committed_entry() {
        ControlPlaneRaftTypeConfig::run(async {
            let mut log_store = ControlPlaneRaftLogStore::empty();
            RaftLogStorage::append(
                &mut log_store,
                vec![
                    bootstrap_membership_entry(1),
                    normal_entry(
                        3,
                        1,
                        1,
                        ControlPlaneCommand::BootstrapInitialClusterMap {
                            nodes: vec![(NodeId::new(1), "node-1".to_string())],
                            pg_ids: vec![PgId::new(0)],
                        },
                    ),
                    normal_entry(
                        3,
                        1,
                        2,
                        ControlPlaneCommand::MarkNodeAvailability {
                            node_id: NodeId::new(99),
                            availability: NodeAvailabilityState::Healthy,
                        },
                    ),
                ],
                IOFlushed::noop(),
            )
            .await
            .unwrap();
            let vote = Vote::<ControlPlaneRaftLeaderId>::new_committed(3, 1);
            RaftLogStorage::save_vote(&mut log_store, &vote)
                .await
                .unwrap();
            RaftLogStorage::save_committed(&mut log_store, Some(raft_log_id(3, 1, 2)))
                .await
                .unwrap();

            let mut state_machine = ControlPlaneRaftStateMachine::empty();
            state_machine
                .apply_entry(bootstrap_membership_entry(1))
                .unwrap();

            let artifact =
                ControlPlaneRaftRestartArtifact::capture(&log_store, &state_machine).unwrap();
            let (_, restored_state_machine) = artifact.restore().unwrap();
            let raft = Raft::<ControlPlaneRaftTypeConfig, ControlPlaneRaftStateMachine>::new(
                1,
                test_raft_config("control-plane-raft-restart-rejection-test"),
                UnreachableRaftNetworkFactory,
                log_store,
                restored_state_machine,
            )
            .await
            .unwrap();

            let applied_state = raft
                .with_state_machine(|state_machine| {
                    let last_applied = state_machine.last_applied();
                    let inner_last_applied = state_machine.inner().last_applied();
                    let node_ids = state_machine
                        .inner()
                        .snapshot()
                        .nodes()
                        .map(|node| node.node_id())
                        .collect::<Vec<_>>();
                    Box::pin(async move { (last_applied, inner_last_applied, node_ids) })
                })
                .await
                .unwrap();
            assert_eq!(applied_state.0, Some(raft_log_id(3, 1, 2)));
            assert_eq!(applied_state.1, Some(ControlPlaneLogId::new(3, 2).unwrap()));
            assert_eq!(applied_state.2, vec![NodeId::new(1)]);

            raft.shutdown().await.unwrap();
        });
    }

    #[test]
    fn control_plane_openraft_restart_restores_current_snapshot() {
        ControlPlaneRaftTypeConfig::run(async {
            let mut log_store = ControlPlaneRaftLogStore::empty();
            RaftLogStorage::append(
                &mut log_store,
                vec![
                    bootstrap_membership_entry(1),
                    normal_entry(
                        3,
                        1,
                        1,
                        ControlPlaneCommand::BootstrapInitialClusterMap {
                            nodes: vec![(NodeId::new(1), "node-1".to_string())],
                            pg_ids: vec![PgId::new(0)],
                        },
                    ),
                    blank_entry(3, 1, 2),
                ],
                IOFlushed::noop(),
            )
            .await
            .unwrap();
            let vote = Vote::<ControlPlaneRaftLeaderId>::new_committed(3, 1);
            RaftLogStorage::save_vote(&mut log_store, &vote)
                .await
                .unwrap();
            RaftLogStorage::save_committed(&mut log_store, Some(raft_log_id(3, 1, 2)))
                .await
                .unwrap();
            RaftLogStorage::purge(&mut log_store, raft_log_id(3, 1, 2))
                .await
                .unwrap();

            let mut snapshot_source = ControlPlaneRaftStateMachine::empty();
            snapshot_source
                .apply_entry(bootstrap_membership_entry(1))
                .unwrap();
            snapshot_source
                .apply_entry(normal_entry(
                    3,
                    1,
                    1,
                    ControlPlaneCommand::BootstrapInitialClusterMap {
                        nodes: vec![(NodeId::new(1), "node-1".to_string())],
                        pg_ids: vec![PgId::new(0)],
                    },
                ))
                .unwrap();
            snapshot_source.apply_entry(blank_entry(3, 1, 2)).unwrap();
            let snapshot = snapshot_source.build_snapshot().unwrap();

            let mut state_machine = ControlPlaneRaftStateMachine::empty();
            state_machine.current_snapshot = Some(snapshot);

            let raft = Raft::<ControlPlaneRaftTypeConfig, ControlPlaneRaftStateMachine>::new(
                1,
                test_raft_config("control-plane-raft-current-snapshot-recovery-test"),
                UnreachableRaftNetworkFactory,
                log_store,
                state_machine,
            )
            .await
            .unwrap();

            let raft_state = raft
                .with_raft_state(|state| {
                    (
                        state.local_committed().cloned(),
                        *state.membership_state.effective().log_id(),
                    )
                })
                .await
                .unwrap();
            assert_eq!(raft_state.0, Some(raft_log_id(3, 1, 2)));
            assert_eq!(raft_state.1, Some(raft_log_id(0, 1, 0)));
            let applied_state = raft
                .with_state_machine(|state_machine| {
                    let last_applied = state_machine.last_applied();
                    let node_ids = state_machine
                        .inner()
                        .snapshot()
                        .nodes()
                        .map(|node| node.node_id())
                        .collect::<Vec<_>>();
                    Box::pin(async move { (last_applied, node_ids) })
                })
                .await
                .unwrap();
            assert_eq!(applied_state.0, Some(raft_log_id(3, 1, 2)));
            assert_eq!(applied_state.1, vec![NodeId::new(1)]);

            raft.shutdown().await.unwrap();
        });
    }

    #[test]
    fn control_plane_raft_log_store_rejects_append_holes() {
        ControlPlaneRaftTypeConfig::run(async {
            let mut store = ControlPlaneRaftLogStore::empty();

            let err =
                RaftLogStorage::append(&mut store, vec![blank_entry(3, 1, 1)], IOFlushed::noop())
                    .await
                    .unwrap_err();
            assert!(err.to_string().contains("expected 0"));
            assert_eq!(
                RaftLogStorage::get_log_state(&mut store)
                    .await
                    .unwrap()
                    .last_log_id,
                None
            );

            RaftLogStorage::append(
                &mut store,
                vec![bootstrap_membership_entry(1)],
                IOFlushed::noop(),
            )
            .await
            .unwrap();
            let err = RaftLogStorage::append(
                &mut store,
                vec![blank_entry(3, 1, 1), blank_entry(3, 1, 3)],
                IOFlushed::noop(),
            )
            .await
            .unwrap_err();
            assert!(err.to_string().contains("log hole at index 2"));
            let entries = RaftLogReader::try_get_log_entries(&mut store, 1..5)
                .await
                .unwrap();
            assert!(entries.is_empty());
            let entries = RaftLogReader::try_get_log_entries(&mut store, 0..5)
                .await
                .unwrap();
            assert_eq!(
                entries.iter().map(|entry| entry.log_id).collect::<Vec<_>>(),
                vec![raft_log_id(0, 1, 0)]
            );
        });
    }

    #[test]
    fn control_plane_raft_log_store_rejects_append_after_max_index() {
        ControlPlaneRaftTypeConfig::run(async {
            let artifact = ControlPlaneRaftLogStoreRestartArtifact {
                vote: Some(Vote::<ControlPlaneRaftLeaderId>::new_committed(3, 1)),
                committed: Some(raft_log_id(3, 1, u64::MAX)),
                last_purged_log_id: Some(raft_log_id(3, 1, u64::MAX)),
                ..Default::default()
            };
            let mut store = ControlPlaneRaftLogStore::from_restart_artifact(artifact).unwrap();

            let err = RaftLogStorage::append(
                &mut store,
                vec![blank_entry(3, 1, u64::MAX)],
                IOFlushed::noop(),
            )
            .await
            .unwrap_err();

            assert!(err
                .to_string()
                .contains("cannot append after u64::MAX OpenRaft log index"));
            let log_state = RaftLogStorage::get_log_state(&mut store).await.unwrap();
            assert_eq!(
                log_state.last_purged_log_id,
                Some(raft_log_id(3, 1, u64::MAX))
            );
            assert_eq!(log_state.last_log_id, Some(raft_log_id(3, 1, u64::MAX)));
        });
    }

    #[test]
    fn control_plane_raft_log_store_purges_and_truncates_without_holes() {
        ControlPlaneRaftTypeConfig::run(async {
            let mut store = ControlPlaneRaftLogStore::empty();
            RaftLogStorage::append(
                &mut store,
                vec![
                    bootstrap_membership_entry(1),
                    blank_entry(3, 1, 1),
                    blank_entry(3, 1, 2),
                    blank_entry(3, 1, 3),
                    blank_entry(3, 1, 4),
                ],
                IOFlushed::noop(),
            )
            .await
            .unwrap();

            let err = RaftLogStorage::purge(&mut store, raft_log_id(3, 1, 2))
                .await
                .unwrap_err();
            assert!(err.to_string().contains("no committed restart gate"));

            let vote = Vote::<ControlPlaneRaftLeaderId>::new_committed(3, 1);
            RaftLogStorage::save_vote(&mut store, &vote).await.unwrap();
            RaftLogStorage::save_committed(&mut store, Some(raft_log_id(3, 1, 2)))
                .await
                .unwrap();
            RaftLogStorage::purge(&mut store, raft_log_id(3, 1, 2))
                .await
                .unwrap();
            let log_state = RaftLogStorage::get_log_state(&mut store).await.unwrap();
            assert_eq!(log_state.last_purged_log_id, Some(raft_log_id(3, 1, 2)));
            assert_eq!(log_state.last_log_id, Some(raft_log_id(3, 1, 4)));

            let entries = RaftLogReader::try_get_log_entries(&mut store, 1..5)
                .await
                .unwrap();
            assert_eq!(
                entries.iter().map(|entry| entry.log_id).collect::<Vec<_>>(),
                vec![raft_log_id(3, 1, 3), raft_log_id(3, 1, 4)]
            );

            RaftLogStorage::truncate_after(&mut store, Some(raft_log_id(3, 1, 3)))
                .await
                .unwrap();
            let log_state = RaftLogStorage::get_log_state(&mut store).await.unwrap();
            assert_eq!(log_state.last_purged_log_id, Some(raft_log_id(3, 1, 2)));
            assert_eq!(log_state.last_log_id, Some(raft_log_id(3, 1, 3)));

            RaftLogStorage::truncate_after(&mut store, Some(raft_log_id(3, 1, 2)))
                .await
                .unwrap();
            let log_state = RaftLogStorage::get_log_state(&mut store).await.unwrap();
            assert_eq!(log_state.last_purged_log_id, Some(raft_log_id(3, 1, 2)));
            assert_eq!(log_state.last_log_id, Some(raft_log_id(3, 1, 2)));

            RaftLogStorage::truncate_after(&mut store, None)
                .await
                .unwrap_err();
            let log_state = RaftLogStorage::get_log_state(&mut store).await.unwrap();
            assert_eq!(log_state.last_purged_log_id, Some(raft_log_id(3, 1, 2)));
            assert_eq!(log_state.last_log_id, Some(raft_log_id(3, 1, 2)));
        });
    }

    #[test]
    fn control_plane_raft_log_store_restores_restart_artifact() {
        ControlPlaneRaftTypeConfig::run(async {
            let mut store = ControlPlaneRaftLogStore::empty();
            let vote = Vote::<ControlPlaneRaftLeaderId>::new_committed(3, 1);
            RaftLogStorage::save_vote(&mut store, &vote).await.unwrap();
            RaftLogStorage::append(
                &mut store,
                vec![
                    bootstrap_membership_entry(1),
                    blank_entry(3, 1, 1),
                    blank_entry(3, 1, 2),
                    blank_entry(3, 1, 3),
                    blank_entry(3, 1, 4),
                ],
                IOFlushed::noop(),
            )
            .await
            .unwrap();
            RaftLogStorage::save_committed(&mut store, Some(raft_log_id(3, 1, 2)))
                .await
                .unwrap();
            RaftLogStorage::purge(&mut store, raft_log_id(3, 1, 2))
                .await
                .unwrap();

            let artifact = store.export_restart_artifact().unwrap();
            let mut restored = ControlPlaneRaftLogStore::from_restart_artifact(artifact).unwrap();

            assert_eq!(
                RaftLogReader::read_vote(&mut restored).await.unwrap(),
                Some(vote)
            );
            assert_eq!(
                RaftLogStorage::read_committed(&mut restored).await.unwrap(),
                Some(raft_log_id(3, 1, 2))
            );
            let log_state = RaftLogStorage::get_log_state(&mut restored).await.unwrap();
            assert_eq!(log_state.last_purged_log_id, Some(raft_log_id(3, 1, 2)));
            assert_eq!(log_state.last_log_id, Some(raft_log_id(3, 1, 4)));

            let entries = RaftLogReader::try_get_log_entries(&mut restored, 0..5)
                .await
                .unwrap();
            assert_eq!(
                entries.iter().map(|entry| entry.log_id).collect::<Vec<_>>(),
                vec![raft_log_id(3, 1, 3), raft_log_id(3, 1, 4)]
            );

            RaftLogStorage::append(&mut restored, vec![blank_entry(3, 1, 5)], IOFlushed::noop())
                .await
                .unwrap();
            let log_state = RaftLogStorage::get_log_state(&mut restored).await.unwrap();
            assert_eq!(log_state.last_log_id, Some(raft_log_id(3, 1, 5)));
        });
    }

    #[test]
    fn control_plane_raft_combined_restart_restores_catchup_state() {
        ControlPlaneRaftTypeConfig::run(async {
            let mut log_store = ControlPlaneRaftLogStore::empty();
            RaftLogStorage::append(
                &mut log_store,
                vec![
                    bootstrap_membership_entry(1),
                    blank_entry(3, 1, 1),
                    blank_entry(3, 1, 2),
                    blank_entry(3, 1, 3),
                ],
                IOFlushed::noop(),
            )
            .await
            .unwrap();
            let vote = Vote::<ControlPlaneRaftLeaderId>::new_committed(3, 1);
            RaftLogStorage::save_vote(&mut log_store, &vote)
                .await
                .unwrap();
            RaftLogStorage::save_committed(&mut log_store, Some(raft_log_id(3, 1, 3)))
                .await
                .unwrap();

            let mut state_machine = ControlPlaneRaftStateMachine::empty();
            state_machine
                .apply_entry(bootstrap_membership_entry(1))
                .unwrap();
            state_machine.apply_entry(blank_entry(3, 1, 1)).unwrap();
            state_machine.apply_entry(blank_entry(3, 1, 2)).unwrap();

            let artifact =
                ControlPlaneRaftRestartArtifact::capture(&log_store, &state_machine).unwrap();
            let (mut restored_log_store, mut restored_state_machine) = artifact.restore().unwrap();

            assert_eq!(
                RaftLogStorage::read_committed(&mut restored_log_store)
                    .await
                    .unwrap(),
                Some(raft_log_id(3, 1, 3))
            );
            assert_eq!(
                restored_state_machine.last_applied(),
                Some(raft_log_id(3, 1, 2))
            );
            restored_state_machine
                .apply_entry(blank_entry(3, 1, 3))
                .unwrap();
            assert_eq!(
                restored_state_machine.last_applied(),
                Some(raft_log_id(3, 1, 3))
            );
        });
    }

    #[test]
    fn control_plane_raft_combined_restart_rejects_inconsistent_artifacts() {
        let log_committed_through_two = ControlPlaneRaftLogStoreRestartArtifact {
            vote: Some(Vote::<ControlPlaneRaftLeaderId>::new_committed(3, 1)),
            committed: Some(raft_log_id(3, 1, 2)),
            entries: vec![
                bootstrap_membership_entry(1),
                blank_entry(3, 1, 1),
                blank_entry(3, 1, 2),
                blank_entry(3, 1, 3),
            ],
            ..Default::default()
        };
        let applied_after_committed = ControlPlaneRaftRestartArtifact {
            log_store: log_committed_through_two.clone(),
            state_machine: state_machine_restart_artifact_with_noops(3, 1, 3),
        };
        let err = applied_after_committed.restore().unwrap_err();
        assert!(err.to_string().contains("after committed restart gate"));

        let missing_committed_gate = ControlPlaneRaftRestartArtifact {
            log_store: ControlPlaneRaftLogStoreRestartArtifact {
                entries: vec![bootstrap_membership_entry(1), blank_entry(3, 1, 1)],
                ..Default::default()
            },
            state_machine: state_machine_restart_artifact_with_noops(3, 1, 1),
        };
        let err = missing_committed_gate.restore().unwrap_err();
        assert!(err.to_string().contains("no committed restart gate"));

        let applied_unknown_to_log = ControlPlaneRaftRestartArtifact {
            log_store: ControlPlaneRaftLogStoreRestartArtifact {
                vote: Some(Vote::<ControlPlaneRaftLeaderId>::new_committed(3, 1)),
                committed: Some(raft_log_id(3, 1, 2)),
                entries: vec![
                    bootstrap_membership_entry(1),
                    blank_entry(3, 1, 1),
                    blank_entry(3, 1, 2),
                ],
                ..Default::default()
            },
            state_machine: state_machine_restart_artifact_with_noops(3, 1, 3),
        };
        let err = applied_unknown_to_log.restore().unwrap_err();
        assert!(err
            .to_string()
            .contains("is not retained or purged in the log store"));

        let state_behind_purged_boundary = ControlPlaneRaftRestartArtifact {
            log_store: ControlPlaneRaftLogStoreRestartArtifact {
                vote: Some(Vote::<ControlPlaneRaftLeaderId>::new_committed(3, 1)),
                committed: Some(raft_log_id(3, 1, 2)),
                last_purged_log_id: Some(raft_log_id(3, 1, 2)),
                entries: Vec::new(),
            },
            state_machine: state_machine_restart_artifact_with_noops(3, 1, 1),
        };
        let err = state_behind_purged_boundary.restore().unwrap_err();
        assert!(err.to_string().contains("behind purged boundary"));
    }

    #[test]
    fn control_plane_raft_log_store_rejects_invalid_restart_artifacts() {
        let artifact_with_entry_at_purged_boundary = ControlPlaneRaftLogStoreRestartArtifact {
            last_purged_log_id: Some(raft_log_id(3, 1, 2)),
            entries: vec![blank_entry(3, 1, 2)],
            ..Default::default()
        };
        let err =
            ControlPlaneRaftLogStore::from_restart_artifact(artifact_with_entry_at_purged_boundary)
                .unwrap_err();
        assert!(err.to_string().contains("expected 3"));

        let artifact_with_log_hole = ControlPlaneRaftLogStoreRestartArtifact {
            entries: vec![
                bootstrap_membership_entry(1),
                blank_entry(3, 1, 1),
                blank_entry(3, 1, 3),
            ],
            ..Default::default()
        };
        let err =
            ControlPlaneRaftLogStore::from_restart_artifact(artifact_with_log_hole).unwrap_err();
        assert!(err.to_string().contains("log hole at index 2"));

        let artifact_with_future_committed = ControlPlaneRaftLogStoreRestartArtifact {
            committed: Some(raft_log_id(3, 1, 3)),
            entries: vec![
                bootstrap_membership_entry(1),
                blank_entry(3, 1, 1),
                blank_entry(3, 1, 2),
            ],
            ..Default::default()
        };
        let err = ControlPlaneRaftLogStore::from_restart_artifact(artifact_with_future_committed)
            .unwrap_err();
        assert!(err.to_string().contains("current last log id"));

        let artifact_with_mismatched_committed = ControlPlaneRaftLogStoreRestartArtifact {
            committed: Some(raft_log_id(4, 1, 2)),
            entries: vec![
                bootstrap_membership_entry(1),
                blank_entry(3, 1, 1),
                blank_entry(3, 1, 2),
            ],
            ..Default::default()
        };
        let err =
            ControlPlaneRaftLogStore::from_restart_artifact(artifact_with_mismatched_committed)
                .unwrap_err();
        assert!(err.to_string().contains("mismatched log id"));

        let artifact_with_committed_before_purge = ControlPlaneRaftLogStoreRestartArtifact {
            committed: Some(raft_log_id(3, 1, 1)),
            last_purged_log_id: Some(raft_log_id(3, 1, 2)),
            entries: vec![blank_entry(3, 1, 3)],
            ..Default::default()
        };
        let err =
            ControlPlaneRaftLogStore::from_restart_artifact(artifact_with_committed_before_purge)
                .unwrap_err();
        assert!(err.to_string().contains("before purged boundary"));

        let artifact_with_missing_vote = ControlPlaneRaftLogStoreRestartArtifact {
            committed: Some(raft_log_id(3, 1, 2)),
            entries: vec![
                bootstrap_membership_entry(1),
                blank_entry(3, 1, 1),
                blank_entry(3, 1, 2),
            ],
            ..Default::default()
        };
        let err = ControlPlaneRaftLogStore::from_restart_artifact(artifact_with_missing_vote)
            .unwrap_err();
        assert!(err.to_string().contains("missing vote state"));

        let artifact_with_stale_vote = ControlPlaneRaftLogStoreRestartArtifact {
            vote: Some(Vote::<ControlPlaneRaftLeaderId>::new(2, 99)),
            committed: Some(raft_log_id(3, 1, 2)),
            entries: vec![
                bootstrap_membership_entry(1),
                blank_entry(3, 1, 1),
                blank_entry(3, 1, 2),
            ],
            ..Default::default()
        };
        let err =
            ControlPlaneRaftLogStore::from_restart_artifact(artifact_with_stale_vote).unwrap_err();
        assert!(err.to_string().contains("does not cover"));

        let artifact_with_same_term_lower_node_vote = ControlPlaneRaftLogStoreRestartArtifact {
            vote: Some(Vote::<ControlPlaneRaftLeaderId>::new_committed(3, 0)),
            committed: Some(raft_log_id(3, 1, 2)),
            entries: vec![
                bootstrap_membership_entry(1),
                blank_entry(3, 1, 1),
                blank_entry(3, 1, 2),
            ],
            ..Default::default()
        };
        let err = ControlPlaneRaftLogStore::from_restart_artifact(
            artifact_with_same_term_lower_node_vote,
        )
        .unwrap_err();
        assert!(err.to_string().contains("does not cover"));
    }
}
