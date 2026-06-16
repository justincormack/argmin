use std::collections::{BTreeMap, BTreeSet};
use std::io::{Read as _, Write as _};
use std::num::NonZeroU64;
use std::os::unix::net::UnixStream;
use std::path::{Path, PathBuf};
use std::time::Duration;

use placement::NodeId;
use thiserror::Error;

use crate::{ClusterEpoch, PgId, PgState};

const CLUSTER_MAP_HISTORY_LIMIT: usize = 32;
pub const MAX_HEARTBEAT_LEASE_MS: u64 = 10_000;
const CONTROL_PLANE_RPC_MAGIC: &[u8] = b"argmin-control-plane-rpc";
const CONTROL_PLANE_RPC_MAX_PAYLOAD_LEN: usize = 8 * 1024 * 1024;
const CONTROL_PLANE_RPC_IO_TIMEOUT: Duration = Duration::from_secs(1);
const CONTROL_PLANE_RPC_HEARTBEAT_OBSERVATION_MIN_LEN: usize = 4 + 1 + 8 + 8 + 8;
const CONTROL_PLANE_RPC_RUNTIME_NODE_MIN_LEN: usize = 4 + 8 + 4;
const CONTROL_PLANE_RPC_PG_ROUTE_MIN_LEN: usize = 8 + 4 + 4 + 1 + 1 + 4;
const CONTROL_PLANE_RPC_ACTING_SET_NODE_MIN_LEN: usize = 4;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct AuthorityIncarnation(NonZeroU64);

impl AuthorityIncarnation {
    pub const INITIAL: Self = Self(NonZeroU64::MIN);

    #[must_use]
    pub fn new(value: u64) -> Option<Self> {
        NonZeroU64::new(value).map(Self)
    }

    #[must_use]
    pub fn get(self) -> u64 {
        self.0.get()
    }

    fn next(self) -> Result<Self, ControlPlaneError> {
        Self::new(
            self.get()
                .checked_add(1)
                .ok_or(ControlPlaneError::AuthorityIncarnationOverflow)?,
        )
        .ok_or(ControlPlaneError::AuthorityIncarnationOverflow)
    }
}

impl std::fmt::Display for AuthorityIncarnation {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.get())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum NodeMembershipState {
    Joining,
    Active,
    Draining,
    Out,
    Removed,
}

impl NodeMembershipState {
    fn as_str(self) -> &'static str {
        match self {
            Self::Joining => "joining",
            Self::Active => "active",
            Self::Draining => "draining",
            Self::Out => "out",
            Self::Removed => "removed",
        }
    }

    fn from_str(value: &str) -> Result<Self, ControlPlaneError> {
        match value {
            "joining" => Ok(Self::Joining),
            "active" => Ok(Self::Active),
            "draining" => Ok(Self::Draining),
            "out" => Ok(Self::Out),
            "removed" => Ok(Self::Removed),
            _ => Err(ControlPlaneError::InvalidState {
                field: "membership",
                value: value.to_owned(),
            }),
        }
    }

    fn can_serve_primary(self) -> bool {
        matches!(self, Self::Active | Self::Draining)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum NodeAvailabilityState {
    Healthy,
    Suspect,
    Unavailable,
}

impl NodeAvailabilityState {
    fn as_str(self) -> &'static str {
        match self {
            Self::Healthy => "healthy",
            Self::Suspect => "suspect",
            Self::Unavailable => "unavailable",
        }
    }

    fn from_str(value: &str) -> Result<Self, ControlPlaneError> {
        match value {
            "healthy" => Ok(Self::Healthy),
            "suspect" => Ok(Self::Suspect),
            "unavailable" => Ok(Self::Unavailable),
            _ => Err(ControlPlaneError::InvalidState {
                field: "availability",
                value: value.to_owned(),
            }),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NodeControlRecord {
    node_id: NodeId,
    membership: NodeMembershipState,
    availability: NodeAvailabilityState,
    node_incarnation: u64,
    endpoint: String,
    last_observed_epoch: Option<ClusterEpoch>,
    last_heartbeat_ms: Option<u64>,
    lease_deadline_ms: Option<u64>,
    pg_observations: BTreeMap<PgId, NodePgObservationRecord>,
}

impl NodeControlRecord {
    fn new(node_id: NodeId, membership: NodeMembershipState) -> Self {
        let availability = if matches!(membership, NodeMembershipState::Removed) {
            NodeAvailabilityState::Unavailable
        } else {
            NodeAvailabilityState::Suspect
        };
        Self {
            node_id,
            membership,
            availability,
            node_incarnation: 0,
            endpoint: String::new(),
            last_observed_epoch: None,
            last_heartbeat_ms: None,
            lease_deadline_ms: None,
            pg_observations: BTreeMap::new(),
        }
    }

    #[must_use]
    pub fn node_id(&self) -> NodeId {
        self.node_id
    }

    #[must_use]
    pub fn membership(&self) -> NodeMembershipState {
        self.membership
    }

    #[must_use]
    pub fn availability(&self) -> NodeAvailabilityState {
        self.availability
    }

    #[must_use]
    pub fn node_incarnation(&self) -> u64 {
        self.node_incarnation
    }

    #[must_use]
    pub fn endpoint(&self) -> &str {
        &self.endpoint
    }

    #[must_use]
    pub fn last_observed_epoch(&self) -> Option<ClusterEpoch> {
        self.last_observed_epoch
    }

    #[must_use]
    pub fn last_heartbeat_ms(&self) -> Option<u64> {
        self.last_heartbeat_ms
    }

    #[must_use]
    pub fn lease_deadline_ms(&self) -> Option<u64> {
        self.lease_deadline_ms
    }

    pub fn pg_observation(&self, pg_id: PgId) -> Option<&NodePgObservationRecord> {
        self.pg_observations.get(&pg_id)
    }

    pub fn pg_observations(&self) -> impl Iterator<Item = &NodePgObservationRecord> {
        self.pg_observations.values()
    }

    fn can_serve_primary(&self, cluster_epoch: ClusterEpoch, now_ms: u64) -> bool {
        self.membership.can_serve_primary()
            && self.availability == NodeAvailabilityState::Healthy
            && self.last_observed_epoch == Some(cluster_epoch)
            && self
                .lease_deadline_ms
                .is_some_and(|lease_deadline_ms| lease_deadline_ms > now_ms)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ClusterControlSnapshot {
    authority_incarnation: AuthorityIncarnation,
    cluster_epoch: ClusterEpoch,
    nodes: BTreeMap<NodeId, NodeControlRecord>,
    pgs: BTreeMap<PgId, PgControlRecord>,
    history: Vec<ClusterMapHistoryRecord>,
}

impl ClusterControlSnapshot {
    fn empty() -> Self {
        Self {
            authority_incarnation: AuthorityIncarnation::INITIAL,
            cluster_epoch: ClusterEpoch::INITIAL,
            nodes: BTreeMap::new(),
            pgs: BTreeMap::new(),
            history: Vec::new(),
        }
    }

    #[must_use]
    pub fn authority_incarnation(&self) -> AuthorityIncarnation {
        self.authority_incarnation
    }

    #[must_use]
    pub fn cluster_epoch(&self) -> ClusterEpoch {
        self.cluster_epoch
    }

    #[must_use]
    pub fn node(&self, node_id: NodeId) -> Option<&NodeControlRecord> {
        self.nodes.get(&node_id)
    }

    pub fn nodes(&self) -> impl Iterator<Item = &NodeControlRecord> {
        self.nodes.values()
    }

    #[must_use]
    pub fn pg(&self, pg_id: PgId) -> Option<&PgControlRecord> {
        self.pgs.get(&pg_id)
    }

    pub fn pgs(&self) -> impl Iterator<Item = &PgControlRecord> {
        self.pgs.values()
    }

    pub fn cluster_map_history(&self) -> &[ClusterMapHistoryRecord] {
        &self.history
    }

    #[must_use]
    pub fn cluster_map_at_epoch(&self, epoch: ClusterEpoch) -> Option<&ClusterMapHistoryRecord> {
        self.history
            .iter()
            .find(|record| record.cluster_epoch == epoch)
    }

    pub fn active_pg_route(
        &self,
        pg_id: PgId,
        now_ms: u64,
    ) -> Result<PgRouteSnapshot, ControlPlaneError> {
        let record = self
            .pg(pg_id)
            .ok_or(ControlPlaneError::UnknownPg { pg_id: pg_id.get() })?;
        if record.state != PgState::Active {
            return Err(ControlPlaneError::PgNotActive {
                pg_id: pg_id.get(),
                cluster_epoch: self.cluster_epoch,
                state: record.state,
            });
        }
        let primary = record
            .active_primary
            .filter(|primary| record.acting_set.contains(primary))
            .ok_or(ControlPlaneError::PgHasNoServingPrimary {
                pg_id: pg_id.get(),
                cluster_epoch: self.cluster_epoch,
            })?;
        let primary_record = self
            .node(primary)
            .ok_or(ControlPlaneError::UnknownActingSetNode {
                pg_id: pg_id.get(),
                node_id: primary.as_u32(),
            })?;
        if !primary_record.can_serve_primary(self.cluster_epoch, now_ms) {
            return Err(ControlPlaneError::PgHasNoServingPrimary {
                pg_id: pg_id.get(),
                cluster_epoch: self.cluster_epoch,
            });
        }
        let primary_lease_deadline_ms = primary_record
            .lease_deadline_ms
            .expect("serving primary must have a lease deadline");
        validate_pg_primary_active_observation(self, pg_id, primary)?;
        Ok(PgRouteSnapshot {
            cluster_epoch: self.cluster_epoch,
            pg_id,
            primary_node_id: primary,
            acting_set: record.acting_set.clone(),
            state: PgState::Active,
            primary_lease_deadline_ms: Some(primary_lease_deadline_ms),
        })
    }

    fn active_pg_route_for_storage_node_refresh(
        &self,
        pg_id: PgId,
        now_ms: u64,
        just_activated: bool,
    ) -> Result<PgRouteSnapshot, ControlPlaneError> {
        let record = self
            .pg(pg_id)
            .ok_or(ControlPlaneError::UnknownPg { pg_id: pg_id.get() })?;
        if record.state != PgState::Active {
            return Err(ControlPlaneError::PgNotActive {
                pg_id: pg_id.get(),
                cluster_epoch: self.cluster_epoch,
                state: record.state,
            });
        }
        let primary = record
            .active_primary
            .filter(|primary| record.acting_set.contains(primary))
            .ok_or(ControlPlaneError::PgHasNoServingPrimary {
                pg_id: pg_id.get(),
                cluster_epoch: self.cluster_epoch,
            })?;
        let primary_record = self
            .node(primary)
            .ok_or(ControlPlaneError::UnknownActingSetNode {
                pg_id: pg_id.get(),
                node_id: primary.as_u32(),
            })?;
        if !primary_record.membership.can_serve_primary()
            || primary_record.availability != NodeAvailabilityState::Healthy
        {
            return Err(ControlPlaneError::PgHasNoServingPrimary {
                pg_id: pg_id.get(),
                cluster_epoch: self.cluster_epoch,
            });
        }
        let Some(primary_lease_deadline_ms) = primary_record.lease_deadline_ms else {
            return Err(ControlPlaneError::PgHasNoServingPrimary {
                pg_id: pg_id.get(),
                cluster_epoch: self.cluster_epoch,
            });
        };
        if primary_lease_deadline_ms <= now_ms {
            return Err(ControlPlaneError::PgHasNoServingPrimary {
                pg_id: pg_id.get(),
                cluster_epoch: self.cluster_epoch,
            });
        }
        if !just_activated {
            validate_pg_primary_active_observation(self, pg_id, primary)?;
        }
        Ok(PgRouteSnapshot {
            cluster_epoch: self.cluster_epoch,
            pg_id,
            primary_node_id: primary,
            acting_set: record.acting_set.clone(),
            state: PgState::Active,
            primary_lease_deadline_ms: Some(primary_lease_deadline_ms),
        })
    }

    pub fn active_pg_routes(&self, now_ms: u64) -> Result<Vec<PgRouteSnapshot>, ControlPlaneError> {
        self.pgs
            .values()
            .filter(|record| record.state == PgState::Active)
            .map(|record| self.active_pg_route(record.pg_id, now_ms))
            .collect()
    }

    pub fn pg_route(&self, pg_id: PgId, now_ms: u64) -> Result<PgRouteSnapshot, ControlPlaneError> {
        let record = self
            .pg(pg_id)
            .ok_or(ControlPlaneError::UnknownPg { pg_id: pg_id.get() })?;
        if record.state == PgState::Active {
            return self.active_pg_route(pg_id, now_ms);
        }
        let primary = record
            .acting_set
            .first()
            .copied()
            .ok_or(ControlPlaneError::EmptyActingSet { pg_id: pg_id.get() })?;
        for &node_id in &record.acting_set {
            if !self.nodes.contains_key(&node_id) {
                return Err(ControlPlaneError::UnknownActingSetNode {
                    pg_id: pg_id.get(),
                    node_id: node_id.as_u32(),
                });
            }
        }
        Ok(PgRouteSnapshot {
            cluster_epoch: self.cluster_epoch,
            pg_id,
            primary_node_id: primary,
            acting_set: record.acting_set.clone(),
            state: record.state,
            primary_lease_deadline_ms: None,
        })
    }

    fn pg_route_for_storage_node_refresh(
        &self,
        pg_id: PgId,
        now_ms: u64,
        just_activated_pgs: &BTreeSet<PgId>,
    ) -> Result<PgRouteSnapshot, ControlPlaneError> {
        let record = self
            .pg(pg_id)
            .ok_or(ControlPlaneError::UnknownPg { pg_id: pg_id.get() })?;
        if record.state == PgState::Active {
            return self.active_pg_route_for_storage_node_refresh(
                pg_id,
                now_ms,
                just_activated_pgs.contains(&pg_id),
            );
        }
        self.pg_route(pg_id, now_ms)
    }

    pub fn pg_routes(&self, now_ms: u64) -> Result<Vec<PgRouteSnapshot>, ControlPlaneError> {
        self.pgs
            .values()
            .map(|record| self.pg_route(record.pg_id, now_ms))
            .collect()
    }

    fn pg_routes_for_storage_node_refresh(
        &self,
        now_ms: u64,
        just_activated_pgs: &BTreeSet<PgId>,
    ) -> Result<Vec<PgRouteSnapshot>, ControlPlaneError> {
        self.pgs
            .values()
            .map(|record| {
                self.pg_route_for_storage_node_refresh(record.pg_id, now_ms, just_activated_pgs)
            })
            .collect()
    }

    pub fn runtime_map(&self, now_ms: u64) -> Result<ClusterRuntimeMapSnapshot, ControlPlaneError> {
        let pg_routes = self.pg_routes(now_ms)?;
        self.runtime_map_from_pg_routes(pg_routes)
    }

    fn runtime_map_for_storage_node_refresh(
        &self,
        now_ms: u64,
        just_activated_pgs: &BTreeSet<PgId>,
    ) -> Result<ClusterRuntimeMapSnapshot, ControlPlaneError> {
        let pg_routes = self.pg_routes_for_storage_node_refresh(now_ms, just_activated_pgs)?;
        self.runtime_map_from_pg_routes(pg_routes)
    }

    fn runtime_map_from_pg_routes(
        &self,
        pg_routes: Vec<PgRouteSnapshot>,
    ) -> Result<ClusterRuntimeMapSnapshot, ControlPlaneError> {
        let mut routed_node_ids = BTreeSet::new();
        for route in &pg_routes {
            routed_node_ids.extend(route.acting_set().iter().copied());
        }
        let mut nodes = Vec::with_capacity(routed_node_ids.len());
        for node_id in routed_node_ids {
            let node = self
                .node(node_id)
                .ok_or(ControlPlaneError::UnknownActingSetNode {
                    pg_id: 0,
                    node_id: node_id.as_u32(),
                })?;
            if node.endpoint.is_empty() {
                return Err(ControlPlaneError::NodeEndpointMissing {
                    node_id: node_id.as_u32(),
                    cluster_epoch: self.cluster_epoch,
                });
            }
            nodes.push(NodeRouteSnapshot {
                node_id,
                node_incarnation: node.node_incarnation,
                endpoint: node.endpoint.clone(),
            });
        }
        let valid_until_ms = pg_routes
            .iter()
            .filter_map(PgRouteSnapshot::primary_lease_deadline_ms)
            .min();
        Ok(ClusterRuntimeMapSnapshot {
            cluster_epoch: self.cluster_epoch,
            valid_until_ms,
            nodes,
            pg_routes,
        })
    }

    fn bump_authority_after_restart(&mut self) -> Result<(), ControlPlaneError> {
        self.authority_incarnation = self.authority_incarnation.next()?;
        self.cluster_epoch = next_epoch(self.cluster_epoch)?;
        for record in self.nodes.values_mut() {
            record.pg_observations.clear();
        }
        Ok(())
    }

    fn bump_epoch(&mut self) -> Result<(), ControlPlaneError> {
        self.cluster_epoch = next_epoch(self.cluster_epoch)?;
        for record in self.nodes.values_mut() {
            record.pg_observations.clear();
        }
        Ok(())
    }

    fn record_history_from(&mut self, previous: &Self) {
        if previous.cluster_epoch == self.cluster_epoch {
            return;
        }
        self.history
            .retain(|record| record.cluster_epoch != previous.cluster_epoch);
        self.history
            .push(ClusterMapHistoryRecord::from_snapshot(previous));
        prune_cluster_map_history(&mut self.history);
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PgRouteSnapshot {
    cluster_epoch: ClusterEpoch,
    pg_id: PgId,
    primary_node_id: NodeId,
    acting_set: Vec<NodeId>,
    state: PgState,
    primary_lease_deadline_ms: Option<u64>,
}

impl PgRouteSnapshot {
    #[must_use]
    pub fn cluster_epoch(&self) -> ClusterEpoch {
        self.cluster_epoch
    }

    #[must_use]
    pub fn pg_id(&self) -> PgId {
        self.pg_id
    }

    #[must_use]
    pub fn primary_node_id(&self) -> NodeId {
        self.primary_node_id
    }

    #[must_use]
    pub fn acting_set(&self) -> &[NodeId] {
        &self.acting_set
    }

    #[must_use]
    pub fn state(&self) -> PgState {
        self.state
    }

    #[must_use]
    pub fn primary_lease_deadline_ms(&self) -> Option<u64> {
        self.primary_lease_deadline_ms
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NodeRouteSnapshot {
    node_id: NodeId,
    node_incarnation: u64,
    endpoint: String,
}

impl NodeRouteSnapshot {
    #[must_use]
    pub fn node_id(&self) -> NodeId {
        self.node_id
    }

    #[must_use]
    pub fn node_incarnation(&self) -> u64 {
        self.node_incarnation
    }

    #[must_use]
    pub fn endpoint(&self) -> &str {
        &self.endpoint
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ClusterRuntimeMapSnapshot {
    cluster_epoch: ClusterEpoch,
    valid_until_ms: Option<u64>,
    nodes: Vec<NodeRouteSnapshot>,
    pg_routes: Vec<PgRouteSnapshot>,
}

impl ClusterRuntimeMapSnapshot {
    #[must_use]
    pub fn cluster_epoch(&self) -> ClusterEpoch {
        self.cluster_epoch
    }

    #[must_use]
    pub fn valid_until_ms(&self) -> Option<u64> {
        self.valid_until_ms
    }

    #[must_use]
    pub fn nodes(&self) -> &[NodeRouteSnapshot] {
        &self.nodes
    }

    #[must_use]
    pub fn pg_routes(&self) -> &[PgRouteSnapshot] {
        &self.pg_routes
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ClusterMapHistoryRecord {
    authority_incarnation: AuthorityIncarnation,
    cluster_epoch: ClusterEpoch,
    nodes: Vec<NodeControlRecord>,
    pgs: Vec<PgControlRecord>,
}

impl ClusterMapHistoryRecord {
    fn from_snapshot(snapshot: &ClusterControlSnapshot) -> Self {
        Self {
            authority_incarnation: snapshot.authority_incarnation,
            cluster_epoch: snapshot.cluster_epoch,
            nodes: snapshot.nodes.values().cloned().collect(),
            pgs: snapshot.pgs.values().cloned().collect(),
        }
    }

    #[must_use]
    pub fn authority_incarnation(&self) -> AuthorityIncarnation {
        self.authority_incarnation
    }

    #[must_use]
    pub fn cluster_epoch(&self) -> ClusterEpoch {
        self.cluster_epoch
    }

    #[must_use]
    pub fn nodes(&self) -> &[NodeControlRecord] {
        &self.nodes
    }

    #[must_use]
    pub fn pgs(&self) -> &[PgControlRecord] {
        &self.pgs
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PgControlRecord {
    pg_id: PgId,
    state: PgState,
    acting_set: Vec<NodeId>,
    active_primary: Option<NodeId>,
    active_metadata_proof: Option<PgMetadataProof>,
}

impl PgControlRecord {
    fn new(pg_id: PgId, acting_set: Vec<NodeId>) -> Self {
        Self {
            pg_id,
            state: PgState::Peering,
            acting_set,
            active_primary: None,
            active_metadata_proof: None,
        }
    }

    #[must_use]
    pub fn pg_id(&self) -> PgId {
        self.pg_id
    }

    #[must_use]
    pub fn state(&self) -> PgState {
        self.state
    }

    #[must_use]
    pub fn acting_set(&self) -> &[NodeId] {
        &self.acting_set
    }

    #[must_use]
    pub fn active_primary(&self) -> Option<NodeId> {
        self.active_primary
    }

    #[must_use]
    pub fn active_metadata_proof(&self) -> Option<PgMetadataProof> {
        self.active_metadata_proof
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct NodePgObservationRecord {
    pg_id: PgId,
    state: PgState,
    observed_epoch: ClusterEpoch,
    observed_at_ms: u64,
    metadata_proof: PgMetadataProof,
}

impl NodePgObservationRecord {
    #[must_use]
    pub fn pg_id(&self) -> PgId {
        self.pg_id
    }

    #[must_use]
    pub fn state(&self) -> PgState {
        self.state
    }

    #[must_use]
    pub fn observed_epoch(&self) -> ClusterEpoch {
        self.observed_epoch
    }

    #[must_use]
    pub fn observed_at_ms(&self) -> u64 {
        self.observed_at_ms
    }

    #[must_use]
    pub fn metadata_proof(&self) -> PgMetadataProof {
        self.metadata_proof
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct NodePgHeartbeatObservation {
    pub pg_id: PgId,
    pub state: PgState,
    pub metadata_proof: PgMetadataProof,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PgMetadataProof {
    pub applied_log_index: u64,
    pub applied_log_hash: u64,
    pub state_digest: u64,
}

impl PgMetadataProof {
    #[must_use]
    pub const fn new(applied_log_index: u64, applied_log_hash: u64, state_digest: u64) -> Self {
        Self {
            applied_log_index,
            applied_log_hash,
            state_digest,
        }
    }

    #[must_use]
    pub const fn empty() -> Self {
        Self::new(0, 0, 0)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NodeHeartbeat {
    pub node_id: NodeId,
    pub node_incarnation: u64,
    pub endpoint: String,
    pub observed_epoch: ClusterEpoch,
    pub requested_lease_duration_ms: u64,
    pub pg_observations: Vec<NodePgHeartbeatObservation>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HeartbeatLease {
    authority_incarnation: AuthorityIncarnation,
    cluster_epoch: ClusterEpoch,
    node_id: NodeId,
    lease_deadline_ms: u64,
    serving: bool,
    snapshot: ClusterControlSnapshot,
}

impl HeartbeatLease {
    #[must_use]
    pub fn authority_incarnation(&self) -> AuthorityIncarnation {
        self.authority_incarnation
    }

    #[must_use]
    pub fn cluster_epoch(&self) -> ClusterEpoch {
        self.cluster_epoch
    }

    #[must_use]
    pub fn node_id(&self) -> NodeId {
        self.node_id
    }

    #[must_use]
    pub fn lease_deadline_ms(&self) -> u64 {
        self.lease_deadline_ms
    }

    #[must_use]
    pub fn serving(&self) -> bool {
        self.serving
    }

    #[must_use]
    pub fn snapshot(&self) -> &ClusterControlSnapshot {
        &self.snapshot
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HeartbeatLeaseExpiry {
    cluster_epoch: ClusterEpoch,
    expired_nodes: Vec<NodeId>,
    peering_pgs: Vec<PgId>,
    snapshot: ClusterControlSnapshot,
}

impl HeartbeatLeaseExpiry {
    #[must_use]
    pub fn cluster_epoch(&self) -> ClusterEpoch {
        self.cluster_epoch
    }

    #[must_use]
    pub fn expired_nodes(&self) -> &[NodeId] {
        &self.expired_nodes
    }

    #[must_use]
    pub fn peering_pgs(&self) -> &[PgId] {
        &self.peering_pgs
    }

    #[must_use]
    pub fn snapshot(&self) -> &ClusterControlSnapshot {
        &self.snapshot
    }
}

pub trait ControlPlaneHeartbeatSink {
    fn submit_node_heartbeat(
        &mut self,
        heartbeat: NodeHeartbeat,
        authority_now_ms: u64,
    ) -> Result<HeartbeatLease, ControlPlaneError>;
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ControlPlaneHeartbeatRefresh {
    lease: HeartbeatLease,
    runtime_map: ClusterRuntimeMapSnapshot,
}

impl ControlPlaneHeartbeatRefresh {
    #[must_use]
    pub fn lease(&self) -> &HeartbeatLease {
        &self.lease
    }

    #[must_use]
    pub fn runtime_map(&self) -> &ClusterRuntimeMapSnapshot {
        &self.runtime_map
    }

    #[must_use]
    pub fn into_parts(self) -> (HeartbeatLease, ClusterRuntimeMapSnapshot) {
        (self.lease, self.runtime_map)
    }
}

pub trait ControlPlaneHeartbeatRuntimeMapSource {
    fn refresh_node_heartbeat(
        &mut self,
        heartbeat: NodeHeartbeat,
        authority_now_ms: u64,
    ) -> Result<ControlPlaneHeartbeatRefresh, ControlPlaneError>;
}

pub trait ControlPlaneRuntimeMapSource {
    fn runtime_map_snapshot(
        &self,
        authority_now_ms: u64,
    ) -> Result<ClusterRuntimeMapSnapshot, ControlPlaneError>;
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct NodeServiceAuthorization {
    authority_incarnation: AuthorityIncarnation,
    cluster_epoch: ClusterEpoch,
    node_id: NodeId,
    node_incarnation: u64,
    lease_deadline_ms: u64,
}

impl NodeServiceAuthorization {
    #[must_use]
    pub fn authority_incarnation(&self) -> AuthorityIncarnation {
        self.authority_incarnation
    }

    #[must_use]
    pub fn cluster_epoch(&self) -> ClusterEpoch {
        self.cluster_epoch
    }

    #[must_use]
    pub fn node_id(&self) -> NodeId {
        self.node_id
    }

    #[must_use]
    pub fn node_incarnation(&self) -> u64 {
        self.node_incarnation
    }

    #[must_use]
    pub fn lease_deadline_ms(&self) -> u64 {
        self.lease_deadline_ms
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PgPrimaryAuthorization {
    authority_incarnation: AuthorityIncarnation,
    cluster_epoch: ClusterEpoch,
    pg_id: PgId,
    primary_node_id: NodeId,
    primary_node_incarnation: u64,
    lease_deadline_ms: u64,
}

impl PgPrimaryAuthorization {
    #[must_use]
    pub fn authority_incarnation(&self) -> AuthorityIncarnation {
        self.authority_incarnation
    }

    #[must_use]
    pub fn cluster_epoch(&self) -> ClusterEpoch {
        self.cluster_epoch
    }

    #[must_use]
    pub fn pg_id(&self) -> PgId {
        self.pg_id
    }

    #[must_use]
    pub fn primary_node_id(&self) -> NodeId {
        self.primary_node_id
    }

    #[must_use]
    pub fn primary_node_incarnation(&self) -> u64 {
        self.primary_node_incarnation
    }

    #[must_use]
    pub fn lease_deadline_ms(&self) -> u64 {
        self.lease_deadline_ms
    }
}

impl From<PgPrimaryAuthorization> for NodeServiceAuthorization {
    fn from(authorization: PgPrimaryAuthorization) -> Self {
        Self {
            authority_incarnation: authorization.authority_incarnation,
            cluster_epoch: authorization.cluster_epoch,
            node_id: authorization.primary_node_id,
            node_incarnation: authorization.primary_node_incarnation,
            lease_deadline_ms: authorization.lease_deadline_ms,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PgServiceOperation {
    MetadataRead,
    MetadataList,
    MetadataWrite,
    PayloadRead,
    PayloadWrite,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PgOperationAuthorization {
    operation: PgServiceOperation,
    primary: PgPrimaryAuthorization,
}

impl PgOperationAuthorization {
    #[must_use]
    pub fn operation(&self) -> PgServiceOperation {
        self.operation
    }

    #[must_use]
    pub fn primary(&self) -> PgPrimaryAuthorization {
        self.primary
    }

    #[must_use]
    pub fn authority_incarnation(&self) -> AuthorityIncarnation {
        self.primary.authority_incarnation()
    }

    #[must_use]
    pub fn cluster_epoch(&self) -> ClusterEpoch {
        self.primary.cluster_epoch()
    }

    #[must_use]
    pub fn pg_id(&self) -> PgId {
        self.primary.pg_id()
    }

    #[must_use]
    pub fn primary_node_id(&self) -> NodeId {
        self.primary.primary_node_id()
    }

    #[must_use]
    pub fn primary_node_incarnation(&self) -> u64 {
        self.primary.primary_node_incarnation()
    }

    #[must_use]
    pub fn lease_deadline_ms(&self) -> u64 {
        self.primary.lease_deadline_ms()
    }
}

pub trait ControlPlaneStore {
    fn load(&self) -> Result<Option<ClusterControlSnapshot>, ControlPlaneError>;
    fn save(&self, snapshot: &ClusterControlSnapshot) -> Result<(), ControlPlaneError>;
}

#[derive(Debug, Clone)]
pub struct FileControlPlaneStore {
    path: PathBuf,
}

impl FileControlPlaneStore {
    #[must_use]
    pub fn new(path: impl Into<PathBuf>) -> Self {
        Self { path: path.into() }
    }

    #[must_use]
    pub fn path(&self) -> &Path {
        &self.path
    }
}

impl ControlPlaneStore for FileControlPlaneStore {
    fn load(&self) -> Result<Option<ClusterControlSnapshot>, ControlPlaneError> {
        match std::fs::read_to_string(&self.path) {
            Ok(contents) => parse_snapshot(&contents).map(Some),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(None),
            Err(source) => Err(ControlPlaneError::Io {
                context: "load control-plane state",
                source,
            }),
        }
    }

    fn save(&self, snapshot: &ClusterControlSnapshot) -> Result<(), ControlPlaneError> {
        if let Some(parent) = state_parent(&self.path) {
            std::fs::create_dir_all(parent).map_err(|source| ControlPlaneError::Io {
                context: "create control-plane state directory",
                source,
            })?;
        }
        let tmp_path = self.path.with_extension("tmp");
        {
            let mut tmp_file =
                std::fs::File::create(&tmp_path).map_err(|source| ControlPlaneError::Io {
                    context: "create control-plane state",
                    source,
                })?;
            tmp_file
                .write_all(format_snapshot(snapshot).as_bytes())
                .map_err(|source| ControlPlaneError::Io {
                    context: "write control-plane state",
                    source,
                })?;
            tmp_file
                .sync_all()
                .map_err(|source| ControlPlaneError::Io {
                    context: "sync control-plane state",
                    source,
                })?;
        }
        std::fs::rename(&tmp_path, &self.path).map_err(|source| ControlPlaneError::Io {
            context: "commit control-plane state",
            source,
        })?;
        if let Some(parent) = state_parent(&self.path) {
            std::fs::File::open(parent)
                .and_then(|directory| directory.sync_all())
                .map_err(|source| ControlPlaneError::Io {
                    context: "sync control-plane state directory",
                    source,
                })?;
        }
        Ok(())
    }
}

#[derive(Debug)]
pub struct SingleAuthorityControlPlane<S> {
    store: S,
    snapshot: ClusterControlSnapshot,
}

impl<S: ControlPlaneStore> SingleAuthorityControlPlane<S> {
    pub fn open(store: S) -> Result<Self, ControlPlaneError> {
        let loaded = store.load()?;
        let loaded_existing_state = loaded.is_some();
        let mut snapshot = loaded.unwrap_or_else(ClusterControlSnapshot::empty);
        if loaded_existing_state {
            let previous_snapshot = snapshot.clone();
            snapshot.bump_authority_after_restart()?;
            snapshot.record_history_from(&previous_snapshot);
        }
        store.save(&snapshot)?;
        Ok(Self { store, snapshot })
    }

    #[must_use]
    pub fn snapshot(&self) -> &ClusterControlSnapshot {
        &self.snapshot
    }

    pub fn set_node_membership(
        &mut self,
        node_id: NodeId,
        membership: NodeMembershipState,
    ) -> Result<ClusterControlSnapshot, ControlPlaneError> {
        let mut next_snapshot = self.snapshot.clone();
        let mut changed = false;
        let mut affected_node = None;
        match next_snapshot.nodes.get_mut(&node_id) {
            Some(record) if record.membership == membership => {}
            Some(record) if record.membership == NodeMembershipState::Removed => {
                return Err(ControlPlaneError::RemovedNodeCannotRejoin {
                    node_id: node_id.as_u32(),
                });
            }
            Some(record) => {
                record.membership = membership;
                if matches!(
                    membership,
                    NodeMembershipState::Out | NodeMembershipState::Removed
                ) {
                    record.availability = NodeAvailabilityState::Unavailable;
                    record.lease_deadline_ms = None;
                    affected_node = Some(node_id);
                }
                changed = true;
            }
            None => {
                next_snapshot
                    .nodes
                    .insert(node_id, NodeControlRecord::new(node_id, membership));
                changed = true;
            }
        }
        if changed {
            if let Some(node_id) = affected_node {
                mark_pgs_peering_for_nodes(&mut next_snapshot, [node_id]);
            }
            next_snapshot.bump_epoch()?;
            self.commit_snapshot(next_snapshot)?;
        }
        Ok(self.snapshot.clone())
    }

    pub fn mark_node_availability(
        &mut self,
        node_id: NodeId,
        availability: NodeAvailabilityState,
    ) -> Result<ClusterControlSnapshot, ControlPlaneError> {
        let mut next_snapshot = self.snapshot.clone();
        let record =
            next_snapshot
                .nodes
                .get_mut(&node_id)
                .ok_or(ControlPlaneError::UnknownNode {
                    node_id: node_id.as_u32(),
                })?;
        if availability == NodeAvailabilityState::Healthy
            && matches!(
                record.membership,
                NodeMembershipState::Out | NodeMembershipState::Removed
            )
        {
            return Err(ControlPlaneError::NodeCannotReceiveLease {
                node_id: node_id.as_u32(),
                membership: record.membership,
            });
        }
        if record.availability != availability {
            record.availability = availability;
            let mut affected_node = None;
            if availability != NodeAvailabilityState::Healthy {
                record.lease_deadline_ms = None;
                affected_node = Some(node_id);
            }
            if let Some(node_id) = affected_node {
                mark_pgs_peering_for_nodes(&mut next_snapshot, [node_id]);
            }
            next_snapshot.bump_epoch()?;
            self.commit_snapshot(next_snapshot)?;
        }
        Ok(self.snapshot.clone())
    }

    pub fn bootstrap_initial_cluster_map(
        &mut self,
        nodes: Vec<(NodeId, String)>,
        pg_ids: Vec<PgId>,
    ) -> Result<ClusterControlSnapshot, ControlPlaneError> {
        if self.snapshot.nodes().next().is_some() || self.snapshot.pgs().next().is_some() {
            return Err(ControlPlaneError::BootstrapRequiresEmptyState);
        }
        if nodes.is_empty() {
            return Err(ControlPlaneError::EmptyActingSet { pg_id: 0 });
        }

        let mut unique_nodes = BTreeSet::new();
        let mut node_ids = Vec::with_capacity(nodes.len());
        for (node_id, endpoint) in &nodes {
            if !unique_nodes.insert(*node_id) {
                return Err(ControlPlaneError::DuplicateActingSetNode {
                    pg_id: 0,
                    node_id: node_id.as_u32(),
                });
            }
            if endpoint.is_empty() {
                return Err(ControlPlaneError::NodeEndpointMissing {
                    node_id: node_id.as_u32(),
                    cluster_epoch: self.snapshot.cluster_epoch(),
                });
            }
            node_ids.push(*node_id);
        }

        let mut unique_pgs = BTreeSet::new();
        for pg_id in &pg_ids {
            if !unique_pgs.insert(*pg_id) {
                return Err(ControlPlaneError::DuplicateBootstrapPg { pg_id: pg_id.get() });
            }
        }

        let mut next_snapshot = self.snapshot.clone();
        for (node_id, endpoint) in nodes {
            let mut record = NodeControlRecord::new(node_id, NodeMembershipState::Active);
            record.endpoint = endpoint;
            next_snapshot.nodes.insert(node_id, record);
        }
        for pg_id in pg_ids {
            next_snapshot
                .pgs
                .insert(pg_id, PgControlRecord::new(pg_id, node_ids.clone()));
        }
        next_snapshot.bump_epoch()?;
        self.commit_snapshot(next_snapshot)?;
        Ok(self.snapshot.clone())
    }

    pub fn set_pg_acting_set(
        &mut self,
        pg_id: PgId,
        acting_set: Vec<NodeId>,
    ) -> Result<ClusterControlSnapshot, ControlPlaneError> {
        validate_acting_set(&self.snapshot, pg_id, &acting_set)?;
        let mut next_snapshot = self.snapshot.clone();
        let mut changed = false;
        match next_snapshot.pgs.get_mut(&pg_id) {
            Some(record) if record.acting_set == acting_set => {}
            Some(record) => {
                record.acting_set = acting_set;
                record.state = PgState::Peering;
                record.active_primary = None;
                record.active_metadata_proof = None;
                changed = true;
            }
            None => {
                next_snapshot
                    .pgs
                    .insert(pg_id, PgControlRecord::new(pg_id, acting_set));
                changed = true;
            }
        }
        if changed {
            next_snapshot.bump_epoch()?;
            self.commit_snapshot(next_snapshot)?;
        }
        Ok(self.snapshot.clone())
    }

    pub fn set_pg_state(
        &mut self,
        pg_id: PgId,
        state: PgState,
    ) -> Result<ClusterControlSnapshot, ControlPlaneError> {
        if state == PgState::Active {
            return Err(ControlPlaneError::ActivePgRequiresPeeringComplete { pg_id: pg_id.get() });
        }
        let mut next_snapshot = self.snapshot.clone();
        let record = next_snapshot
            .pgs
            .get_mut(&pg_id)
            .ok_or(ControlPlaneError::UnknownPg { pg_id: pg_id.get() })?;
        if record.state != state {
            record.state = state;
            record.active_primary = None;
            record.active_metadata_proof = None;
            next_snapshot.bump_epoch()?;
            self.commit_snapshot(next_snapshot)?;
        }
        Ok(self.snapshot.clone())
    }

    pub fn complete_pg_peering(
        &mut self,
        pg_id: PgId,
        primary: NodeId,
        node_incarnation: u64,
        now_ms: u64,
    ) -> Result<ClusterControlSnapshot, ControlPlaneError> {
        let record = self
            .snapshot
            .pg(pg_id)
            .ok_or(ControlPlaneError::UnknownPg { pg_id: pg_id.get() })?;
        if !record.acting_set.contains(&primary) {
            return Err(ControlPlaneError::PgPrimaryNotInActingSet {
                pg_id: pg_id.get(),
                node_id: primary.as_u32(),
            });
        }
        self.authorize_node_service(
            primary,
            node_incarnation,
            self.snapshot.cluster_epoch,
            now_ms,
        )?;
        if record.state == PgState::Active {
            if record.active_primary == Some(primary) {
                return Ok(self.snapshot.clone());
            }
            return Err(ControlPlaneError::PgNotPeering {
                pg_id: pg_id.get(),
                cluster_epoch: self.snapshot.cluster_epoch,
                state: record.state,
            });
        }
        if self.deterministic_pg_primary(pg_id, record.acting_set(), now_ms) != Some(primary) {
            return Err(ControlPlaneError::PgPrimaryNotServingCurrentEpoch {
                pg_id: pg_id.get(),
                node_id: primary.as_u32(),
            });
        }
        if record.state != PgState::Peering {
            return Err(ControlPlaneError::PgNotPeering {
                pg_id: pg_id.get(),
                cluster_epoch: self.snapshot.cluster_epoch,
                state: record.state,
            });
        }
        let active_metadata_proof =
            validate_pg_peering_observations(&self.snapshot, pg_id, record.acting_set(), now_ms)?;

        let mut next_snapshot = self.snapshot.clone();
        let record = next_snapshot
            .pgs
            .get_mut(&pg_id)
            .expect("PG record validated before peering completion");
        record.state = PgState::Active;
        record.active_primary = Some(primary);
        record.active_metadata_proof = Some(active_metadata_proof);
        next_snapshot.bump_epoch()?;
        self.commit_snapshot(next_snapshot)?;
        Ok(self.snapshot.clone())
    }

    pub fn complete_ready_pg_peerings(
        &mut self,
        now_ms: u64,
    ) -> Result<Vec<PgId>, ControlPlaneError> {
        let mut ready = Vec::new();
        for record in self.snapshot.pgs.values() {
            if record.state != PgState::Peering {
                continue;
            }
            let Some(primary) =
                self.deterministic_pg_primary(record.pg_id, record.acting_set(), now_ms)
            else {
                continue;
            };
            match validate_pg_peering_observations(
                &self.snapshot,
                record.pg_id,
                record.acting_set(),
                now_ms,
            ) {
                Ok(active_metadata_proof) => {
                    ready.push((record.pg_id, primary, active_metadata_proof))
                }
                Err(
                    ControlPlaneError::PgPeeringMissingObservation { .. }
                    | ControlPlaneError::PgPeeringObservationNotPeering { .. }
                    | ControlPlaneError::PgPeeringMetadataProofMismatch { .. },
                ) => {}
                Err(error) => return Err(error),
            }
        }
        if ready.is_empty() {
            return Ok(Vec::new());
        }

        let mut next_snapshot = self.snapshot.clone();
        for (pg_id, primary, active_metadata_proof) in &ready {
            let record = next_snapshot
                .pgs
                .get_mut(pg_id)
                .expect("ready PG must exist in cloned snapshot");
            record.state = PgState::Active;
            record.active_primary = Some(*primary);
            record.active_metadata_proof = Some(*active_metadata_proof);
        }
        next_snapshot.bump_epoch()?;
        self.commit_snapshot(next_snapshot)?;
        Ok(ready.into_iter().map(|(pg_id, _, _)| pg_id).collect())
    }

    pub fn heartbeat(
        &mut self,
        heartbeat: NodeHeartbeat,
        authority_now_ms: u64,
    ) -> Result<HeartbeatLease, ControlPlaneError> {
        if heartbeat.requested_lease_duration_ms == 0 {
            return Err(ControlPlaneError::InvalidLeaseDuration);
        }
        if heartbeat.requested_lease_duration_ms > MAX_HEARTBEAT_LEASE_MS {
            return Err(ControlPlaneError::LeaseDurationTooLong {
                requested_ms: heartbeat.requested_lease_duration_ms,
                max_ms: MAX_HEARTBEAT_LEASE_MS,
            });
        }
        let lease_deadline_ms = authority_now_ms
            .checked_add(heartbeat.requested_lease_duration_ms)
            .ok_or(ControlPlaneError::LeaseDeadlineOverflow)?;
        let current_epoch = self.snapshot.cluster_epoch;

        let record =
            self.snapshot
                .nodes
                .get(&heartbeat.node_id)
                .ok_or(ControlPlaneError::UnknownNode {
                    node_id: heartbeat.node_id.as_u32(),
                })?;
        if matches!(
            record.membership,
            NodeMembershipState::Out | NodeMembershipState::Removed
        ) {
            return Err(ControlPlaneError::NodeCannotReceiveLease {
                node_id: heartbeat.node_id.as_u32(),
                membership: record.membership,
            });
        }
        if heartbeat.node_incarnation < record.node_incarnation {
            return Err(ControlPlaneError::StaleNodeIncarnation {
                node_id: heartbeat.node_id.as_u32(),
                heartbeat_incarnation: heartbeat.node_incarnation,
                current_incarnation: record.node_incarnation,
            });
        }
        if heartbeat.observed_epoch != current_epoch {
            return Ok(HeartbeatLease {
                authority_incarnation: self.snapshot.authority_incarnation,
                cluster_epoch: current_epoch,
                node_id: heartbeat.node_id,
                lease_deadline_ms: record.lease_deadline_ms.unwrap_or(authority_now_ms),
                serving: false,
                snapshot: self.snapshot.clone(),
            });
        }
        validate_pg_heartbeat_observations(
            &self.snapshot,
            heartbeat.node_id,
            &heartbeat.pg_observations,
        )?;

        let mut epoch_changed = false;
        let mut affected_node = None;
        let mut next_snapshot = self.snapshot.clone();
        {
            let record = next_snapshot
                .nodes
                .get_mut(&heartbeat.node_id)
                .expect("node record validated before heartbeat mutation");
            if heartbeat.node_incarnation > record.node_incarnation {
                record.node_incarnation = heartbeat.node_incarnation;
                epoch_changed = true;
                affected_node = Some(heartbeat.node_id);
            }
            if record.endpoint != heartbeat.endpoint {
                record.endpoint = heartbeat.endpoint;
                epoch_changed = true;
                affected_node = Some(heartbeat.node_id);
            }
            if record.availability != NodeAvailabilityState::Healthy {
                record.availability = NodeAvailabilityState::Healthy;
                epoch_changed = true;
                affected_node = Some(heartbeat.node_id);
            }
            record.last_observed_epoch = Some(heartbeat.observed_epoch);
            record.last_heartbeat_ms = Some(authority_now_ms);
            record.lease_deadline_ms = Some(lease_deadline_ms);
            record.pg_observations.clear();
            for observation in &heartbeat.pg_observations {
                record.pg_observations.insert(
                    observation.pg_id,
                    NodePgObservationRecord {
                        pg_id: observation.pg_id,
                        state: observation.state,
                        observed_epoch: heartbeat.observed_epoch,
                        observed_at_ms: authority_now_ms,
                        metadata_proof: observation.metadata_proof,
                    },
                );
            }
        }
        if epoch_changed {
            if let Some(node_id) = affected_node {
                mark_pgs_peering_for_nodes(&mut next_snapshot, [node_id]);
            }
            next_snapshot.bump_epoch()?;
        }
        self.commit_snapshot(next_snapshot)?;
        let serving = self.snapshot.node(heartbeat.node_id).is_some_and(|record| {
            record.can_serve_primary(self.snapshot.cluster_epoch, authority_now_ms)
        });
        Ok(HeartbeatLease {
            authority_incarnation: self.snapshot.authority_incarnation,
            cluster_epoch: self.snapshot.cluster_epoch,
            node_id: heartbeat.node_id,
            lease_deadline_ms,
            serving,
            snapshot: self.snapshot.clone(),
        })
    }

    fn current_heartbeat_lease_for_node(
        &self,
        node_id: NodeId,
        now_ms: u64,
    ) -> Result<HeartbeatLease, ControlPlaneError> {
        let record = self
            .snapshot
            .node(node_id)
            .ok_or(ControlPlaneError::UnknownNode {
                node_id: node_id.as_u32(),
            })?;
        Ok(HeartbeatLease {
            authority_incarnation: self.snapshot.authority_incarnation,
            cluster_epoch: self.snapshot.cluster_epoch,
            node_id,
            lease_deadline_ms: record.lease_deadline_ms.unwrap_or(now_ms),
            serving: record.can_serve_primary(self.snapshot.cluster_epoch, now_ms),
            snapshot: self.snapshot.clone(),
        })
    }

    pub fn expire_heartbeat_leases(
        &mut self,
        now_ms: u64,
    ) -> Result<HeartbeatLeaseExpiry, ControlPlaneError> {
        let mut next_snapshot = self.snapshot.clone();
        let mut expired_nodes = Vec::new();
        for record in next_snapshot.nodes.values_mut() {
            if matches!(
                record.membership,
                NodeMembershipState::Out | NodeMembershipState::Removed
            ) || record.availability == NodeAvailabilityState::Unavailable
            {
                continue;
            }
            if record
                .lease_deadline_ms
                .is_some_and(|lease_deadline_ms| lease_deadline_ms <= now_ms)
            {
                record.availability = NodeAvailabilityState::Unavailable;
                record.lease_deadline_ms = None;
                expired_nodes.push(record.node_id);
            }
        }
        if !expired_nodes.is_empty() {
            let peering_pgs = mark_pgs_peering_for_nodes(&mut next_snapshot, expired_nodes.clone());
            next_snapshot.bump_epoch()?;
            self.commit_snapshot(next_snapshot)?;
            return Ok(HeartbeatLeaseExpiry {
                cluster_epoch: self.snapshot.cluster_epoch,
                expired_nodes,
                peering_pgs,
                snapshot: self.snapshot.clone(),
            });
        }
        Ok(HeartbeatLeaseExpiry {
            cluster_epoch: self.snapshot.cluster_epoch,
            expired_nodes,
            peering_pgs: Vec::new(),
            snapshot: self.snapshot.clone(),
        })
    }

    pub fn authorize_node_service(
        &self,
        node_id: NodeId,
        node_incarnation: u64,
        observed_epoch: ClusterEpoch,
        now_ms: u64,
    ) -> Result<NodeServiceAuthorization, ControlPlaneError> {
        let record = self
            .snapshot
            .nodes
            .get(&node_id)
            .ok_or(ControlPlaneError::UnknownNode {
                node_id: node_id.as_u32(),
            })?;
        if matches!(
            record.membership,
            NodeMembershipState::Out | NodeMembershipState::Removed
        ) {
            return Err(ControlPlaneError::NodeCannotReceiveLease {
                node_id: node_id.as_u32(),
                membership: record.membership,
            });
        }
        if node_incarnation != record.node_incarnation {
            return Err(ControlPlaneError::NodeIncarnationMismatch {
                node_id: node_id.as_u32(),
                sender_incarnation: node_incarnation,
                current_incarnation: record.node_incarnation,
            });
        }
        if observed_epoch != self.snapshot.cluster_epoch {
            return Err(ControlPlaneError::StaleNodeObservedEpoch {
                node_id: node_id.as_u32(),
                observed_epoch,
                current_epoch: self.snapshot.cluster_epoch,
            });
        }
        let lease_deadline_ms =
            record
                .lease_deadline_ms
                .ok_or(ControlPlaneError::NodeLeaseExpired {
                    node_id: node_id.as_u32(),
                    now_ms,
                    lease_deadline_ms: None,
                })?;
        if lease_deadline_ms <= now_ms {
            return Err(ControlPlaneError::NodeLeaseExpired {
                node_id: node_id.as_u32(),
                now_ms,
                lease_deadline_ms: Some(lease_deadline_ms),
            });
        }
        if !record.can_serve_primary(self.snapshot.cluster_epoch, now_ms) {
            return Err(ControlPlaneError::NodeNotServingCurrentEpoch {
                node_id: node_id.as_u32(),
                cluster_epoch: self.snapshot.cluster_epoch,
            });
        }
        Ok(NodeServiceAuthorization {
            authority_incarnation: self.snapshot.authority_incarnation,
            cluster_epoch: self.snapshot.cluster_epoch,
            node_id,
            node_incarnation,
            lease_deadline_ms,
        })
    }

    pub fn validate_node_service_authorization(
        &self,
        authorization: &NodeServiceAuthorization,
        now_ms: u64,
    ) -> Result<(), ControlPlaneError> {
        if authorization.authority_incarnation() != self.snapshot.authority_incarnation {
            return Err(ControlPlaneError::StaleAuthorityIncarnation {
                authority_incarnation: authorization.authority_incarnation(),
                current_authority_incarnation: self.snapshot.authority_incarnation,
            });
        }
        if authorization.cluster_epoch() != self.snapshot.cluster_epoch {
            return Err(ControlPlaneError::StaleAuthorizationEpoch {
                cluster_epoch: authorization.cluster_epoch(),
                current_epoch: self.snapshot.cluster_epoch,
            });
        }
        if authorization.lease_deadline_ms() <= now_ms {
            return Err(ControlPlaneError::NodeLeaseExpired {
                node_id: authorization.node_id().as_u32(),
                now_ms,
                lease_deadline_ms: Some(authorization.lease_deadline_ms()),
            });
        }

        let node_id = authorization.node_id();
        let record = self
            .snapshot
            .nodes
            .get(&node_id)
            .ok_or(ControlPlaneError::UnknownNode {
                node_id: node_id.as_u32(),
            })?;
        if record.node_incarnation != authorization.node_incarnation() {
            return Err(ControlPlaneError::NodeIncarnationMismatch {
                node_id: node_id.as_u32(),
                sender_incarnation: authorization.node_incarnation(),
                current_incarnation: record.node_incarnation,
            });
        }
        let current_lease_deadline_ms =
            record
                .lease_deadline_ms
                .ok_or(ControlPlaneError::NodeLeaseExpired {
                    node_id: node_id.as_u32(),
                    now_ms,
                    lease_deadline_ms: None,
                })?;
        if current_lease_deadline_ms <= now_ms {
            return Err(ControlPlaneError::NodeLeaseExpired {
                node_id: node_id.as_u32(),
                now_ms,
                lease_deadline_ms: Some(current_lease_deadline_ms),
            });
        }
        if !record.can_serve_primary(self.snapshot.cluster_epoch, now_ms) {
            return Err(ControlPlaneError::NodeNotServingCurrentEpoch {
                node_id: node_id.as_u32(),
                cluster_epoch: self.snapshot.cluster_epoch,
            });
        }
        Ok(())
    }

    pub fn authorize_pg_primary_service(
        &self,
        pg_id: PgId,
        primary_node_id: NodeId,
        node_incarnation: u64,
        observed_epoch: ClusterEpoch,
        now_ms: u64,
    ) -> Result<PgPrimaryAuthorization, ControlPlaneError> {
        let node_authorization =
            self.authorize_node_service(primary_node_id, node_incarnation, observed_epoch, now_ms)?;
        let record = self
            .snapshot
            .pgs
            .get(&pg_id)
            .ok_or(ControlPlaneError::UnknownPg { pg_id: pg_id.get() })?;
        if record.state != PgState::Active {
            return Err(ControlPlaneError::PgNotActive {
                pg_id: pg_id.get(),
                cluster_epoch: self.snapshot.cluster_epoch,
                state: record.state,
            });
        }
        let serving_primary = record
            .active_primary
            .filter(|primary| {
                self.snapshot
                    .node(*primary)
                    .is_some_and(|node| node.can_serve_primary(self.snapshot.cluster_epoch, now_ms))
            })
            .ok_or(ControlPlaneError::PgHasNoServingPrimary {
                pg_id: pg_id.get(),
                cluster_epoch: self.snapshot.cluster_epoch,
            })?;
        if serving_primary != primary_node_id {
            return Err(ControlPlaneError::NodeNotPgPrimary {
                pg_id: pg_id.get(),
                node_id: primary_node_id.as_u32(),
                primary_node_id: serving_primary.as_u32(),
                cluster_epoch: self.snapshot.cluster_epoch,
            });
        }
        validate_pg_primary_active_observation(&self.snapshot, pg_id, primary_node_id)?;
        Ok(PgPrimaryAuthorization {
            authority_incarnation: self.snapshot.authority_incarnation,
            cluster_epoch: self.snapshot.cluster_epoch,
            pg_id,
            primary_node_id,
            primary_node_incarnation: node_authorization.node_incarnation(),
            lease_deadline_ms: node_authorization.lease_deadline_ms(),
        })
    }

    pub fn authorize_pg_operation(
        &self,
        operation: PgServiceOperation,
        pg_id: PgId,
        primary_node_id: NodeId,
        node_incarnation: u64,
        observed_epoch: ClusterEpoch,
        now_ms: u64,
    ) -> Result<PgOperationAuthorization, ControlPlaneError> {
        let primary = self.authorize_pg_primary_service(
            pg_id,
            primary_node_id,
            node_incarnation,
            observed_epoch,
            now_ms,
        )?;
        Ok(PgOperationAuthorization { operation, primary })
    }

    pub fn validate_pg_operation_authorization(
        &self,
        authorization: &PgOperationAuthorization,
        now_ms: u64,
    ) -> Result<(), ControlPlaneError> {
        self.validate_pg_operation_authorization_for(
            authorization,
            authorization.operation(),
            now_ms,
        )
    }

    pub fn validate_pg_operation_authorization_for(
        &self,
        authorization: &PgOperationAuthorization,
        operation: PgServiceOperation,
        now_ms: u64,
    ) -> Result<(), ControlPlaneError> {
        if authorization.operation() != operation {
            return Err(ControlPlaneError::PgOperationAuthorizationMismatch {
                expected: operation,
                actual: authorization.operation(),
            });
        }
        self.validate_node_service_authorization(&authorization.primary().into(), now_ms)?;

        let primary_node_id = authorization.primary_node_id();
        let pg_id = authorization.pg_id();
        let pg = self
            .snapshot
            .pgs
            .get(&pg_id)
            .ok_or(ControlPlaneError::UnknownPg { pg_id: pg_id.get() })?;
        if pg.state != PgState::Active {
            return Err(ControlPlaneError::PgNotActive {
                pg_id: pg_id.get(),
                cluster_epoch: self.snapshot.cluster_epoch,
                state: pg.state,
            });
        }
        let serving_primary = pg
            .active_primary
            .filter(|primary| {
                self.snapshot
                    .node(*primary)
                    .is_some_and(|node| node.can_serve_primary(self.snapshot.cluster_epoch, now_ms))
            })
            .ok_or(ControlPlaneError::PgHasNoServingPrimary {
                pg_id: pg_id.get(),
                cluster_epoch: self.snapshot.cluster_epoch,
            })?;
        if serving_primary != primary_node_id {
            return Err(ControlPlaneError::NodeNotPgPrimary {
                pg_id: pg_id.get(),
                node_id: primary_node_id.as_u32(),
                primary_node_id: serving_primary.as_u32(),
                cluster_epoch: self.snapshot.cluster_epoch,
            });
        }
        validate_pg_primary_active_observation(&self.snapshot, pg_id, primary_node_id)
    }

    #[must_use]
    pub fn serving_pg_primary(&self, pg_id: PgId, now_ms: u64) -> Option<NodeId> {
        let record = self.snapshot.pgs.get(&pg_id)?;
        if record.state != PgState::Active {
            return None;
        }
        let primary = record.active_primary?;
        self.snapshot
            .node(primary)
            .is_some_and(|node| node.can_serve_primary(self.snapshot.cluster_epoch, now_ms))
            .then_some(())?;
        primary_has_current_pg_state(&self.snapshot, pg_id, primary, PgState::Active)
            .then_some(primary)
    }

    #[must_use]
    pub fn deterministic_pg_primary(
        &self,
        _pg_id: PgId,
        acting_set: &[NodeId],
        now_ms: u64,
    ) -> Option<NodeId> {
        acting_set.iter().copied().find(|node_id| {
            self.snapshot
                .nodes
                .get(node_id)
                .is_some_and(|record| record.can_serve_primary(self.snapshot.cluster_epoch, now_ms))
        })
    }

    fn commit_snapshot(
        &mut self,
        mut next_snapshot: ClusterControlSnapshot,
    ) -> Result<(), ControlPlaneError> {
        next_snapshot.record_history_from(&self.snapshot);
        self.store.save(&next_snapshot)?;
        self.snapshot = next_snapshot;
        Ok(())
    }
}

impl<S: ControlPlaneStore> ControlPlaneHeartbeatSink for SingleAuthorityControlPlane<S> {
    fn submit_node_heartbeat(
        &mut self,
        heartbeat: NodeHeartbeat,
        authority_now_ms: u64,
    ) -> Result<HeartbeatLease, ControlPlaneError> {
        self.heartbeat(heartbeat, authority_now_ms)
    }
}

impl<S: ControlPlaneStore> ControlPlaneHeartbeatRuntimeMapSource
    for SingleAuthorityControlPlane<S>
{
    fn refresh_node_heartbeat(
        &mut self,
        heartbeat: NodeHeartbeat,
        authority_now_ms: u64,
    ) -> Result<ControlPlaneHeartbeatRefresh, ControlPlaneError> {
        let node_id = heartbeat.node_id;
        let mut lease = self.heartbeat(heartbeat, authority_now_ms)?;
        let just_activated_pgs: BTreeSet<PgId> = self
            .complete_ready_pg_peerings(authority_now_ms)?
            .into_iter()
            .collect();
        if !just_activated_pgs.is_empty() {
            lease = self.current_heartbeat_lease_for_node(node_id, authority_now_ms)?;
        }
        let runtime_map = self
            .snapshot
            .runtime_map_for_storage_node_refresh(authority_now_ms, &just_activated_pgs)?;
        Ok(ControlPlaneHeartbeatRefresh { lease, runtime_map })
    }
}

impl<S: ControlPlaneStore> ControlPlaneRuntimeMapSource for SingleAuthorityControlPlane<S> {
    fn runtime_map_snapshot(
        &self,
        authority_now_ms: u64,
    ) -> Result<ClusterRuntimeMapSnapshot, ControlPlaneError> {
        self.snapshot.runtime_map(authority_now_ms)
    }
}

#[derive(Debug, Clone)]
pub struct UnixControlPlaneClient {
    socket_path: PathBuf,
}

impl UnixControlPlaneClient {
    #[must_use]
    pub fn new(socket_path: impl Into<PathBuf>) -> Self {
        Self {
            socket_path: socket_path.into(),
        }
    }

    #[must_use]
    pub fn socket_path(&self) -> &Path {
        &self.socket_path
    }

    fn send_request(
        &self,
        kind: ControlPlaneRpcKind,
        payload: &[u8],
    ) -> Result<Vec<u8>, ControlPlaneError> {
        let mut stream =
            UnixStream::connect(&self.socket_path).map_err(|source| ControlPlaneError::Io {
                context: "connect control-plane socket",
                source,
            })?;
        stream
            .set_read_timeout(Some(CONTROL_PLANE_RPC_IO_TIMEOUT))
            .map_err(|source| ControlPlaneError::Io {
                context: "set control-plane client read timeout",
                source,
            })?;
        stream
            .set_write_timeout(Some(CONTROL_PLANE_RPC_IO_TIMEOUT))
            .map_err(|source| ControlPlaneError::Io {
                context: "set control-plane client write timeout",
                source,
            })?;
        write_control_plane_rpc_frame(&mut stream, kind, payload)?;
        let (response_kind, response_payload) = read_control_plane_rpc_frame(&mut stream)?;
        if response_kind != kind {
            return Err(ControlPlaneError::RpcProtocol {
                message: format!(
                    "response kind {:?} did not match request kind {:?}",
                    response_kind, kind
                ),
            });
        }
        decode_control_plane_rpc_response(response_payload)
    }
}

impl ControlPlaneRuntimeMapSource for UnixControlPlaneClient {
    fn runtime_map_snapshot(
        &self,
        _authority_now_ms: u64,
    ) -> Result<ClusterRuntimeMapSnapshot, ControlPlaneError> {
        let payload = self.send_request(ControlPlaneRpcKind::RuntimeMapSnapshot, &[])?;
        let mut reader = PayloadReader::new(&payload);
        let runtime_map = read_runtime_map_snapshot(&mut reader)?;
        reader.finish()?;
        Ok(runtime_map)
    }
}

impl ControlPlaneHeartbeatRuntimeMapSource for UnixControlPlaneClient {
    fn refresh_node_heartbeat(
        &mut self,
        heartbeat: NodeHeartbeat,
        _authority_now_ms: u64,
    ) -> Result<ControlPlaneHeartbeatRefresh, ControlPlaneError> {
        let mut payload = Vec::new();
        write_node_heartbeat(&mut payload, &heartbeat)?;
        let payload = self.send_request(ControlPlaneRpcKind::RefreshNodeHeartbeat, &payload)?;
        let mut reader = PayloadReader::new(&payload);
        let lease = read_heartbeat_lease(&mut reader)?;
        let runtime_map = read_runtime_map_snapshot(&mut reader)?;
        reader.finish()?;
        Ok(ControlPlaneHeartbeatRefresh { lease, runtime_map })
    }
}

pub fn handle_control_plane_unix_stream<T>(
    control_plane: &mut T,
    stream: &mut UnixStream,
    authority_now_ms: u64,
) -> Result<(), ControlPlaneError>
where
    T: ControlPlaneHeartbeatRuntimeMapSource + ControlPlaneRuntimeMapSource,
{
    let request = read_control_plane_unix_request(stream)?;
    let response = build_control_plane_unix_response(control_plane, request, authority_now_ms)?;
    write_control_plane_unix_response(stream, response)
}

#[derive(Debug)]
pub struct ControlPlaneRpcRequest {
    kind: ControlPlaneRpcKind,
    payload: Vec<u8>,
}

#[derive(Debug)]
pub struct ControlPlaneRpcResponse {
    kind: ControlPlaneRpcKind,
    payload: Vec<u8>,
}

pub fn read_control_plane_unix_request(
    stream: &mut UnixStream,
) -> Result<ControlPlaneRpcRequest, ControlPlaneError> {
    let (kind, payload) = read_control_plane_rpc_frame(stream)?;
    Ok(ControlPlaneRpcRequest { kind, payload })
}

pub fn build_control_plane_unix_response<T>(
    control_plane: &mut T,
    request: ControlPlaneRpcRequest,
    authority_now_ms: u64,
) -> Result<ControlPlaneRpcResponse, ControlPlaneError>
where
    T: ControlPlaneHeartbeatRuntimeMapSource + ControlPlaneRuntimeMapSource,
{
    let ControlPlaneRpcRequest { kind, payload } = request;
    let response = match kind {
        ControlPlaneRpcKind::RuntimeMapSnapshot => {
            let reader = PayloadReader::new(&payload);
            reader.finish()?;
            match control_plane.runtime_map_snapshot(authority_now_ms) {
                Ok(snapshot) => {
                    let mut response = Vec::new();
                    write_runtime_map_snapshot(&mut response, &snapshot)?;
                    Ok(response)
                }
                Err(error) => Err(error),
            }
        }
        ControlPlaneRpcKind::RefreshNodeHeartbeat => {
            let mut reader = PayloadReader::new(&payload);
            let heartbeat = read_node_heartbeat(&mut reader)?;
            reader.finish()?;
            match control_plane.refresh_node_heartbeat(heartbeat, authority_now_ms) {
                Ok(refresh) => {
                    let mut response = Vec::new();
                    write_heartbeat_lease(&mut response, refresh.lease())?;
                    write_runtime_map_snapshot(&mut response, refresh.runtime_map())?;
                    Ok(response)
                }
                Err(error) => Err(error),
            }
        }
    };
    let payload = encode_control_plane_rpc_response(response)?;
    Ok(ControlPlaneRpcResponse { kind, payload })
}

pub fn write_control_plane_unix_response(
    stream: &mut UnixStream,
    response: ControlPlaneRpcResponse,
) -> Result<(), ControlPlaneError> {
    write_control_plane_rpc_frame(stream, response.kind, &response.payload)
}

pub fn respond_control_plane_unix_request<T>(
    control_plane: &mut T,
    stream: &mut UnixStream,
    request: ControlPlaneRpcRequest,
    authority_now_ms: u64,
) -> Result<(), ControlPlaneError>
where
    T: ControlPlaneHeartbeatRuntimeMapSource + ControlPlaneRuntimeMapSource,
{
    let response = build_control_plane_unix_response(control_plane, request, authority_now_ms)?;
    write_control_plane_unix_response(stream, response)
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ControlPlaneRpcKind {
    RuntimeMapSnapshot = 1,
    RefreshNodeHeartbeat = 2,
}

impl ControlPlaneRpcKind {
    fn from_u16(value: u16) -> Result<Self, ControlPlaneError> {
        match value {
            1 => Ok(Self::RuntimeMapSnapshot),
            2 => Ok(Self::RefreshNodeHeartbeat),
            _ => Err(ControlPlaneError::RpcProtocol {
                message: format!("unknown control-plane RPC kind {value}"),
            }),
        }
    }
}

fn write_control_plane_rpc_frame(
    stream: &mut UnixStream,
    kind: ControlPlaneRpcKind,
    payload: &[u8],
) -> Result<(), ControlPlaneError> {
    let payload_len = u32::try_from(payload.len()).map_err(|_| ControlPlaneError::RpcProtocol {
        message: format!("control-plane RPC payload too large: {}", payload.len()),
    })?;
    stream
        .write_all(CONTROL_PLANE_RPC_MAGIC)
        .map_err(|source| ControlPlaneError::Io {
            context: "write control-plane RPC magic",
            source,
        })?;
    let mut header = Vec::with_capacity(8);
    write_u16(&mut header, 1);
    write_u16(&mut header, kind as u16);
    write_u32(&mut header, payload_len);
    write_u64(
        &mut header,
        control_plane_rpc_frame_checksum(1, kind as u16, payload_len, payload),
    );
    stream
        .write_all(&header)
        .and_then(|()| stream.write_all(payload))
        .map_err(|source| ControlPlaneError::Io {
            context: "write control-plane RPC frame",
            source,
        })
}

fn read_control_plane_rpc_frame(
    stream: &mut UnixStream,
) -> Result<(ControlPlaneRpcKind, Vec<u8>), ControlPlaneError> {
    let mut magic = vec![0; CONTROL_PLANE_RPC_MAGIC.len()];
    stream
        .read_exact(&mut magic)
        .map_err(|source| ControlPlaneError::Io {
            context: "read control-plane RPC magic",
            source,
        })?;
    if magic != CONTROL_PLANE_RPC_MAGIC {
        return Err(ControlPlaneError::RpcProtocol {
            message: "invalid control-plane RPC magic".to_owned(),
        });
    }
    let mut header = [0; 16];
    stream
        .read_exact(&mut header)
        .map_err(|source| ControlPlaneError::Io {
            context: "read control-plane RPC header",
            source,
        })?;
    let mut reader = PayloadReader::new(&header);
    let version = reader.read_u16()?;
    if version != 1 {
        return Err(ControlPlaneError::RpcProtocol {
            message: format!("unsupported control-plane RPC version {version}"),
        });
    }
    let kind = ControlPlaneRpcKind::from_u16(reader.read_u16()?)?;
    let raw_kind = kind as u16;
    let payload_len_u32 = reader.read_u32()?;
    let payload_len =
        usize::try_from(payload_len_u32).map_err(|_| ControlPlaneError::RpcProtocol {
            message: "control-plane RPC payload length does not fit usize".to_owned(),
        })?;
    let expected_checksum = reader.read_u64()?;
    reader.finish()?;
    if payload_len > CONTROL_PLANE_RPC_MAX_PAYLOAD_LEN {
        return Err(ControlPlaneError::RpcProtocol {
            message: format!("control-plane RPC payload too large: {payload_len}"),
        });
    }
    let mut payload = vec![0; payload_len];
    stream
        .read_exact(&mut payload)
        .map_err(|source| ControlPlaneError::Io {
            context: "read control-plane RPC payload",
            source,
        })?;
    if control_plane_rpc_frame_checksum(version, raw_kind, payload_len_u32, &payload)
        != expected_checksum
    {
        return Err(ControlPlaneError::RpcProtocol {
            message: "control-plane RPC frame checksum mismatch".to_owned(),
        });
    }
    Ok((kind, payload))
}

fn control_plane_rpc_frame_checksum(
    version: u16,
    raw_kind: u16,
    payload_len: u32,
    payload: &[u8],
) -> u64 {
    let mut hasher = checksum::crc64::Hasher::new();
    hasher.update(CONTROL_PLANE_RPC_MAGIC);
    hasher.update(&version.to_le_bytes());
    hasher.update(&raw_kind.to_le_bytes());
    hasher.update(&payload_len.to_le_bytes());
    hasher.update(payload);
    hasher.finalize()
}

fn encode_control_plane_rpc_response(
    response: Result<Vec<u8>, ControlPlaneError>,
) -> Result<Vec<u8>, ControlPlaneError> {
    let mut payload = Vec::new();
    match response {
        Ok(response) => {
            write_u8(&mut payload, 0);
            write_bytes(&mut payload, &response)?;
        }
        Err(error) => {
            write_u8(&mut payload, 1);
            write_string(&mut payload, &error.to_string())?;
        }
    }
    Ok(payload)
}

fn decode_control_plane_rpc_response(payload: Vec<u8>) -> Result<Vec<u8>, ControlPlaneError> {
    let mut reader = PayloadReader::new(&payload);
    let status = reader.read_u8()?;
    match status {
        0 => {
            let response = reader.read_bytes()?.to_vec();
            reader.finish()?;
            Ok(response)
        }
        1 => {
            let message = reader.read_string()?.to_owned();
            reader.finish()?;
            Err(ControlPlaneError::RpcRemote { message })
        }
        _ => Err(ControlPlaneError::RpcProtocol {
            message: format!("invalid control-plane RPC response status {status}"),
        }),
    }
}

fn write_node_heartbeat(
    out: &mut Vec<u8>,
    heartbeat: &NodeHeartbeat,
) -> Result<(), ControlPlaneError> {
    write_u32(out, heartbeat.node_id.as_u32());
    write_u64(out, heartbeat.node_incarnation);
    write_string(out, &heartbeat.endpoint)?;
    write_u64(out, heartbeat.observed_epoch.get());
    write_u64(out, heartbeat.requested_lease_duration_ms);
    write_u32(
        out,
        len_as_u32(heartbeat.pg_observations.len(), "PG observations")?,
    );
    for observation in &heartbeat.pg_observations {
        write_u32(out, observation.pg_id.get());
        write_pg_state(out, observation.state);
        write_pg_metadata_proof(out, observation.metadata_proof);
    }
    Ok(())
}

fn read_node_heartbeat(reader: &mut PayloadReader<'_>) -> Result<NodeHeartbeat, ControlPlaneError> {
    let node_id = NodeId::new(reader.read_u32()?);
    let node_incarnation = reader.read_u64()?;
    let endpoint = reader.read_string()?.to_owned();
    let observed_epoch = read_cluster_epoch(reader, "heartbeat observed epoch")?;
    let requested_lease_duration_ms = reader.read_u64()?;
    let observation_count = reader.read_collection_len(
        "PG observations",
        CONTROL_PLANE_RPC_HEARTBEAT_OBSERVATION_MIN_LEN,
    )?;
    let mut pg_observations = Vec::with_capacity(observation_count);
    for _ in 0..observation_count {
        pg_observations.push(NodePgHeartbeatObservation {
            pg_id: PgId::new(reader.read_u32()?),
            state: read_pg_state(reader)?,
            metadata_proof: read_pg_metadata_proof(reader)?,
        });
    }
    Ok(NodeHeartbeat {
        node_id,
        node_incarnation,
        endpoint,
        observed_epoch,
        requested_lease_duration_ms,
        pg_observations,
    })
}

fn write_heartbeat_lease(
    out: &mut Vec<u8>,
    lease: &HeartbeatLease,
) -> Result<(), ControlPlaneError> {
    write_u64(out, lease.authority_incarnation().get());
    write_u64(out, lease.cluster_epoch().get());
    write_u32(out, lease.node_id().as_u32());
    write_u64(out, lease.lease_deadline_ms());
    write_u8(out, u8::from(lease.serving()));
    write_string(out, &format_snapshot(lease.snapshot()))
}

fn read_heartbeat_lease(
    reader: &mut PayloadReader<'_>,
) -> Result<HeartbeatLease, ControlPlaneError> {
    let authority_incarnation = AuthorityIncarnation::new(reader.read_u64()?).ok_or_else(|| {
        ControlPlaneError::RpcProtocol {
            message: "heartbeat lease authority incarnation must be nonzero".to_owned(),
        }
    })?;
    let cluster_epoch = read_cluster_epoch(reader, "heartbeat lease cluster epoch")?;
    let node_id = NodeId::new(reader.read_u32()?);
    let lease_deadline_ms = reader.read_u64()?;
    let serving = reader.read_bool()?;
    let snapshot = parse_snapshot(reader.read_string()?)?;
    Ok(HeartbeatLease {
        authority_incarnation,
        cluster_epoch,
        node_id,
        lease_deadline_ms,
        serving,
        snapshot,
    })
}

fn write_runtime_map_snapshot(
    out: &mut Vec<u8>,
    snapshot: &ClusterRuntimeMapSnapshot,
) -> Result<(), ControlPlaneError> {
    write_u64(out, snapshot.cluster_epoch().get());
    write_option_u64(out, snapshot.valid_until_ms());
    write_u32(out, len_as_u32(snapshot.nodes().len(), "runtime nodes")?);
    for node in snapshot.nodes() {
        write_u32(out, node.node_id().as_u32());
        write_u64(out, node.node_incarnation());
        write_string(out, node.endpoint())?;
    }
    write_u32(out, len_as_u32(snapshot.pg_routes().len(), "PG routes")?);
    for route in snapshot.pg_routes() {
        write_u64(out, route.cluster_epoch().get());
        write_u32(out, route.pg_id().get());
        write_u32(out, route.primary_node_id().as_u32());
        write_pg_state(out, route.state());
        write_option_u64(out, route.primary_lease_deadline_ms());
        write_u32(out, len_as_u32(route.acting_set().len(), "acting set")?);
        for node_id in route.acting_set() {
            write_u32(out, node_id.as_u32());
        }
    }
    Ok(())
}

fn read_runtime_map_snapshot(
    reader: &mut PayloadReader<'_>,
) -> Result<ClusterRuntimeMapSnapshot, ControlPlaneError> {
    let cluster_epoch = read_cluster_epoch(reader, "runtime map cluster epoch")?;
    let valid_until_ms = reader.read_option_u64()?;
    let node_count =
        reader.read_collection_len("runtime nodes", CONTROL_PLANE_RPC_RUNTIME_NODE_MIN_LEN)?;
    let mut nodes = Vec::with_capacity(node_count);
    for _ in 0..node_count {
        nodes.push(NodeRouteSnapshot {
            node_id: NodeId::new(reader.read_u32()?),
            node_incarnation: reader.read_u64()?,
            endpoint: reader.read_string()?.to_owned(),
        });
    }
    let route_count =
        reader.read_collection_len("PG routes", CONTROL_PLANE_RPC_PG_ROUTE_MIN_LEN)?;
    let mut pg_routes = Vec::with_capacity(route_count);
    for _ in 0..route_count {
        let route_epoch = read_cluster_epoch(reader, "PG route cluster epoch")?;
        let pg_id = PgId::new(reader.read_u32()?);
        let primary_node_id = NodeId::new(reader.read_u32()?);
        let state = read_pg_state(reader)?;
        let primary_lease_deadline_ms = reader.read_option_u64()?;
        let acting_set_len = reader.read_collection_len(
            "PG route acting set",
            CONTROL_PLANE_RPC_ACTING_SET_NODE_MIN_LEN,
        )?;
        let mut acting_set = Vec::with_capacity(acting_set_len);
        for _ in 0..acting_set_len {
            acting_set.push(NodeId::new(reader.read_u32()?));
        }
        pg_routes.push(PgRouteSnapshot {
            cluster_epoch: route_epoch,
            pg_id,
            primary_node_id,
            acting_set,
            state,
            primary_lease_deadline_ms,
        });
    }
    Ok(ClusterRuntimeMapSnapshot {
        cluster_epoch,
        valid_until_ms,
        nodes,
        pg_routes,
    })
}

fn write_pg_metadata_proof(out: &mut Vec<u8>, proof: PgMetadataProof) {
    write_u64(out, proof.applied_log_index);
    write_u64(out, proof.applied_log_hash);
    write_u64(out, proof.state_digest);
}

fn read_pg_metadata_proof(
    reader: &mut PayloadReader<'_>,
) -> Result<PgMetadataProof, ControlPlaneError> {
    Ok(PgMetadataProof {
        applied_log_index: reader.read_u64()?,
        applied_log_hash: reader.read_u64()?,
        state_digest: reader.read_u64()?,
    })
}

fn write_pg_state(out: &mut Vec<u8>, state: PgState) {
    write_u8(
        out,
        match state {
            PgState::Active => 1,
            PgState::Peering => 2,
            PgState::Degraded => 3,
            PgState::Backfilling => 4,
            PgState::Inconsistent => 5,
        },
    );
}

fn read_pg_state(reader: &mut PayloadReader<'_>) -> Result<PgState, ControlPlaneError> {
    match reader.read_u8()? {
        1 => Ok(PgState::Active),
        2 => Ok(PgState::Peering),
        3 => Ok(PgState::Degraded),
        4 => Ok(PgState::Backfilling),
        5 => Ok(PgState::Inconsistent),
        state => Err(ControlPlaneError::RpcProtocol {
            message: format!("invalid PG state code {state}"),
        }),
    }
}

fn read_cluster_epoch(
    reader: &mut PayloadReader<'_>,
    field: &'static str,
) -> Result<ClusterEpoch, ControlPlaneError> {
    ClusterEpoch::new(reader.read_u64()?).ok_or_else(|| ControlPlaneError::RpcProtocol {
        message: format!("{field} must be nonzero"),
    })
}

fn write_option_u64(out: &mut Vec<u8>, value: Option<u64>) {
    match value {
        Some(value) => {
            write_u8(out, 1);
            write_u64(out, value);
        }
        None => write_u8(out, 0),
    }
}

fn write_string(out: &mut Vec<u8>, value: &str) -> Result<(), ControlPlaneError> {
    write_bytes(out, value.as_bytes())
}

fn write_bytes(out: &mut Vec<u8>, value: &[u8]) -> Result<(), ControlPlaneError> {
    write_u32(out, len_as_u32(value.len(), "byte field")?);
    out.extend_from_slice(value);
    Ok(())
}

fn write_u8(out: &mut Vec<u8>, value: u8) {
    out.push(value);
}

fn write_u16(out: &mut Vec<u8>, value: u16) {
    out.extend_from_slice(&value.to_be_bytes());
}

fn write_u32(out: &mut Vec<u8>, value: u32) {
    out.extend_from_slice(&value.to_be_bytes());
}

fn write_u64(out: &mut Vec<u8>, value: u64) {
    out.extend_from_slice(&value.to_be_bytes());
}

fn len_as_u32(len: usize, field: &'static str) -> Result<u32, ControlPlaneError> {
    u32::try_from(len).map_err(|_| ControlPlaneError::RpcProtocol {
        message: format!("{field} length {len} exceeds u32::MAX"),
    })
}

struct PayloadReader<'a> {
    payload: &'a [u8],
    offset: usize,
}

impl<'a> PayloadReader<'a> {
    fn new(payload: &'a [u8]) -> Self {
        Self { payload, offset: 0 }
    }

    fn finish(&self) -> Result<(), ControlPlaneError> {
        if self.offset == self.payload.len() {
            Ok(())
        } else {
            Err(ControlPlaneError::RpcProtocol {
                message: format!(
                    "control-plane RPC payload has {} trailing bytes",
                    self.payload.len() - self.offset
                ),
            })
        }
    }

    fn read_exact(&mut self, len: usize) -> Result<&'a [u8], ControlPlaneError> {
        let end = self
            .offset
            .checked_add(len)
            .ok_or_else(|| ControlPlaneError::RpcProtocol {
                message: "control-plane RPC payload offset overflow".to_owned(),
            })?;
        let bytes =
            self.payload
                .get(self.offset..end)
                .ok_or_else(|| ControlPlaneError::RpcProtocol {
                    message: "truncated control-plane RPC payload".to_owned(),
                })?;
        self.offset = end;
        Ok(bytes)
    }

    fn read_u8(&mut self) -> Result<u8, ControlPlaneError> {
        Ok(self.read_exact(1)?[0])
    }

    fn read_u16(&mut self) -> Result<u16, ControlPlaneError> {
        let bytes = self.read_exact(2)?;
        Ok(u16::from_be_bytes([bytes[0], bytes[1]]))
    }

    fn read_u32(&mut self) -> Result<u32, ControlPlaneError> {
        let bytes = self.read_exact(4)?;
        Ok(u32::from_be_bytes([bytes[0], bytes[1], bytes[2], bytes[3]]))
    }

    fn read_u64(&mut self) -> Result<u64, ControlPlaneError> {
        let bytes = self.read_exact(8)?;
        Ok(u64::from_be_bytes([
            bytes[0], bytes[1], bytes[2], bytes[3], bytes[4], bytes[5], bytes[6], bytes[7],
        ]))
    }

    fn read_bool(&mut self) -> Result<bool, ControlPlaneError> {
        match self.read_u8()? {
            0 => Ok(false),
            1 => Ok(true),
            value => Err(ControlPlaneError::RpcProtocol {
                message: format!("invalid boolean value {value}"),
            }),
        }
    }

    fn read_option_u64(&mut self) -> Result<Option<u64>, ControlPlaneError> {
        match self.read_u8()? {
            0 => Ok(None),
            1 => Ok(Some(self.read_u64()?)),
            value => Err(ControlPlaneError::RpcProtocol {
                message: format!("invalid optional u64 tag {value}"),
            }),
        }
    }

    fn read_len(&mut self, field: &'static str) -> Result<usize, ControlPlaneError> {
        usize::try_from(self.read_u32()?).map_err(|_| ControlPlaneError::RpcProtocol {
            message: format!("{field} length does not fit usize"),
        })
    }

    fn read_collection_len(
        &mut self,
        field: &'static str,
        min_item_len: usize,
    ) -> Result<usize, ControlPlaneError> {
        assert!(min_item_len > 0);
        let len = self.read_len(field)?;
        let max_items = self.remaining_len() / min_item_len;
        if len > max_items {
            return Err(ControlPlaneError::RpcProtocol {
                message: format!(
                    "{field} count {len} exceeds remaining control-plane RPC payload capacity {max_items}",
                ),
            });
        }
        Ok(len)
    }

    fn read_bytes(&mut self) -> Result<&'a [u8], ControlPlaneError> {
        let len = self.read_len("byte field")?;
        self.read_exact(len)
    }

    fn read_string(&mut self) -> Result<&'a str, ControlPlaneError> {
        std::str::from_utf8(self.read_bytes()?).map_err(|source| ControlPlaneError::RpcProtocol {
            message: format!("control-plane RPC string is not UTF-8: {source}"),
        })
    }

    fn remaining_len(&self) -> usize {
        self.payload.len() - self.offset
    }
}

#[derive(Debug, Error)]
pub enum ControlPlaneError {
    #[error("{context}: {source}")]
    Io {
        context: &'static str,
        #[source]
        source: std::io::Error,
    },

    #[error("control-plane state parse error at line {line}: {message}")]
    Parse { line: usize, message: String },

    #[error("control-plane RPC protocol error: {message}")]
    RpcProtocol { message: String },

    #[error("control-plane RPC remote error: {message}")]
    RpcRemote { message: String },

    #[error("invalid {field} state {value:?}")]
    InvalidState { field: &'static str, value: String },

    #[error("unknown node {node_id}")]
    UnknownNode { node_id: u32 },

    #[error("unknown PG {pg_id}")]
    UnknownPg { pg_id: u32 },

    #[error("PG {pg_id} acting set must not be empty")]
    EmptyActingSet { pg_id: u32 },

    #[error("PG {pg_id} acting set references unknown node {node_id}")]
    UnknownActingSetNode { pg_id: u32, node_id: u32 },

    #[error("PG {pg_id} acting set repeats node {node_id}")]
    DuplicateActingSetNode { pg_id: u32, node_id: u32 },

    #[error("control-plane bootstrap repeats PG {pg_id}")]
    DuplicateBootstrapPg { pg_id: u32 },

    #[error("control-plane bootstrap requires empty state")]
    BootstrapRequiresEmptyState,

    #[error("node {node_id} heartbeat repeats PG {pg_id} observation")]
    DuplicatePgObservation { node_id: u32, pg_id: u32 },

    #[error("node {node_id} heartbeat reports PG {pg_id} outside its acting set")]
    PgObservationNotInActingSet { node_id: u32, pg_id: u32 },

    #[error("PG {pg_id} Active state requires complete_pg_peering")]
    ActivePgRequiresPeeringComplete { pg_id: u32 },

    #[error("PG {pg_id} Active state is missing its accepted metadata proof")]
    ActivePgMissingMetadataProof { pg_id: u32 },

    #[error("node {node_id} is not in PG {pg_id} acting set")]
    PgPrimaryNotInActingSet { pg_id: u32, node_id: u32 },

    #[error("node {node_id} is not serving current epoch as PG {pg_id} primary")]
    PgPrimaryNotServingCurrentEpoch { pg_id: u32, node_id: u32 },

    #[error(
        "node {node_id} incarnation {sender_incarnation} does not match current incarnation {current_incarnation}"
    )]
    NodeIncarnationMismatch {
        node_id: u32,
        sender_incarnation: u64,
        current_incarnation: u64,
    },

    #[error("node {node_id} observed epoch {observed_epoch}, current epoch is {current_epoch}")]
    StaleNodeObservedEpoch {
        node_id: u32,
        observed_epoch: ClusterEpoch,
        current_epoch: ClusterEpoch,
    },

    #[error(
        "authorization authority incarnation {authority_incarnation} is stale; current incarnation is {current_authority_incarnation}"
    )]
    StaleAuthorityIncarnation {
        authority_incarnation: AuthorityIncarnation,
        current_authority_incarnation: AuthorityIncarnation,
    },

    #[error(
        "authorization cluster epoch {cluster_epoch} is stale; current epoch is {current_epoch}"
    )]
    StaleAuthorizationEpoch {
        cluster_epoch: ClusterEpoch,
        current_epoch: ClusterEpoch,
    },

    #[error("authorization is for {actual:?}, not expected operation {expected:?}")]
    PgOperationAuthorizationMismatch {
        expected: PgServiceOperation,
        actual: PgServiceOperation,
    },

    #[error("node {node_id} is not serving cluster epoch {cluster_epoch}")]
    NodeNotServingCurrentEpoch {
        node_id: u32,
        cluster_epoch: ClusterEpoch,
    },

    #[error("node {node_id} has no advertised endpoint in cluster epoch {cluster_epoch}")]
    NodeEndpointMissing {
        node_id: u32,
        cluster_epoch: ClusterEpoch,
    },

    #[error(
        "node {node_id} lease expired at {lease_deadline_ms:?}; authorization time is {now_ms}"
    )]
    NodeLeaseExpired {
        node_id: u32,
        now_ms: u64,
        lease_deadline_ms: Option<u64>,
    },

    #[error("PG {pg_id} is {state} in cluster epoch {cluster_epoch}, not active")]
    PgNotActive {
        pg_id: u32,
        cluster_epoch: ClusterEpoch,
        state: PgState,
    },

    #[error("PG {pg_id} is {state} in cluster epoch {cluster_epoch}, not peering")]
    PgNotPeering {
        pg_id: u32,
        cluster_epoch: ClusterEpoch,
        state: PgState,
    },

    #[error(
        "node {node_id} has not reported PG {pg_id} peering state in cluster epoch {cluster_epoch}"
    )]
    PgPeeringMissingObservation {
        pg_id: u32,
        node_id: u32,
        cluster_epoch: ClusterEpoch,
    },

    #[error(
        "node {node_id} reported PG {pg_id} as {state} in cluster epoch {cluster_epoch}, not peering"
    )]
    PgPeeringObservationNotPeering {
        pg_id: u32,
        node_id: u32,
        cluster_epoch: ClusterEpoch,
        state: PgState,
    },

    #[error(
        "node {node_id} reported PG {pg_id} metadata proof {actual:?} in cluster epoch {cluster_epoch}, expected {expected:?}"
    )]
    PgPeeringMetadataProofMismatch {
        pg_id: u32,
        node_id: u32,
        cluster_epoch: ClusterEpoch,
        expected: PgMetadataProof,
        actual: PgMetadataProof,
    },

    #[error(
        "node {node_id} reported Active PG {pg_id} metadata proof {actual:?} in cluster epoch {cluster_epoch}, expected active proof {expected:?}"
    )]
    PgActiveMetadataProofMismatch {
        pg_id: u32,
        node_id: u32,
        cluster_epoch: ClusterEpoch,
        expected: PgMetadataProof,
        actual: PgMetadataProof,
    },

    #[error(
        "PG {pg_id} primary node {node_id} has not reported active state in cluster epoch {cluster_epoch}"
    )]
    PgPrimaryMissingActiveObservation {
        pg_id: u32,
        node_id: u32,
        cluster_epoch: ClusterEpoch,
    },

    #[error(
        "PG {pg_id} primary node {node_id} reported {state} in cluster epoch {cluster_epoch}, not active"
    )]
    PgPrimaryObservationNotActive {
        pg_id: u32,
        node_id: u32,
        cluster_epoch: ClusterEpoch,
        state: PgState,
    },

    #[error("PG {pg_id} has no serving primary in cluster epoch {cluster_epoch}")]
    PgHasNoServingPrimary {
        pg_id: u32,
        cluster_epoch: ClusterEpoch,
    },

    #[error(
        "node {node_id} is not PG {pg_id} primary in cluster epoch {cluster_epoch}; primary is {primary_node_id}"
    )]
    NodeNotPgPrimary {
        pg_id: u32,
        node_id: u32,
        primary_node_id: u32,
        cluster_epoch: ClusterEpoch,
    },

    #[error("removed node {node_id} cannot rejoin")]
    RemovedNodeCannotRejoin { node_id: u32 },

    #[error("node {node_id} in membership state {membership:?} cannot receive a lease")]
    NodeCannotReceiveLease {
        node_id: u32,
        membership: NodeMembershipState,
    },

    #[error(
        "stale heartbeat from node {node_id}: incarnation {heartbeat_incarnation} is older than current {current_incarnation}"
    )]
    StaleNodeIncarnation {
        node_id: u32,
        heartbeat_incarnation: u64,
        current_incarnation: u64,
    },

    #[error("heartbeat lease duration must be positive")]
    InvalidLeaseDuration,

    #[error("heartbeat lease duration {requested_ms}ms exceeds maximum {max_ms}ms")]
    LeaseDurationTooLong { requested_ms: u64, max_ms: u64 },

    #[error("heartbeat lease deadline overflow")]
    LeaseDeadlineOverflow,

    #[error("cluster epoch overflow")]
    ClusterEpochOverflow,

    #[error("authority incarnation overflow")]
    AuthorityIncarnationOverflow,
}

fn next_epoch(epoch: ClusterEpoch) -> Result<ClusterEpoch, ControlPlaneError> {
    ClusterEpoch::new(
        epoch
            .get()
            .checked_add(1)
            .ok_or(ControlPlaneError::ClusterEpochOverflow)?,
    )
    .ok_or(ControlPlaneError::ClusterEpochOverflow)
}

fn format_snapshot(snapshot: &ClusterControlSnapshot) -> String {
    let mut out = String::new();
    out.push_str("version=7\n");
    out.push_str(&format!(
        "authority_incarnation={}\n",
        snapshot.authority_incarnation.get()
    ));
    out.push_str(&format!("cluster_epoch={}\n", snapshot.cluster_epoch.get()));
    for history in &snapshot.history {
        out.push_str(&format!(
            "history={},{}\n",
            history.cluster_epoch.get(),
            history.authority_incarnation.get()
        ));
        for record in &history.nodes {
            out.push_str(&format!(
                "history_node={},{}\n",
                history.cluster_epoch.get(),
                format_node_record(record)
            ));
            for observation in record.pg_observations.values() {
                out.push_str(&format!(
                    "history_node_pg={},{}\n",
                    history.cluster_epoch.get(),
                    format_node_pg_record(record.node_id, observation)
                ));
            }
        }
        for record in &history.pgs {
            out.push_str(&format!(
                "history_pg={},{}\n",
                history.cluster_epoch.get(),
                format_pg_record(record)
            ));
        }
    }
    for record in snapshot.nodes.values() {
        out.push_str(&format!("node={}\n", format_node_record(record)));
        for observation in record.pg_observations.values() {
            out.push_str(&format!(
                "node_pg={}\n",
                format_node_pg_record(record.node_id, observation)
            ));
        }
    }
    for record in snapshot.pgs.values() {
        out.push_str(&format!("pg={}\n", format_pg_record(record)));
    }
    out
}

fn format_node_record(record: &NodeControlRecord) -> String {
    format!(
        "{},{},{},{},{},{},{},{}",
        record.node_id.as_u32(),
        record.membership.as_str(),
        record.availability.as_str(),
        record.node_incarnation,
        option_u64(record.last_observed_epoch.map(ClusterEpoch::get)),
        option_u64(record.last_heartbeat_ms),
        option_u64(record.lease_deadline_ms),
        hex_encode(record.endpoint.as_bytes())
    )
}

fn format_pg_record(record: &PgControlRecord) -> String {
    let (active_log_index, active_log_hash, active_state_digest) =
        match record.active_metadata_proof {
            Some(proof) => (
                option_u64(Some(proof.applied_log_index)),
                option_u64(Some(proof.applied_log_hash)),
                option_u64(Some(proof.state_digest)),
            ),
            None => (option_u64(None), option_u64(None), option_u64(None)),
        };
    format!(
        "{},{},{},{},{},{},{}",
        record.pg_id.get(),
        pg_state_as_str(record.state),
        format_node_list(&record.acting_set),
        option_u32(record.active_primary.map(NodeId::as_u32)),
        active_log_index,
        active_log_hash,
        active_state_digest
    )
}

fn format_node_pg_record(node_id: NodeId, record: &NodePgObservationRecord) -> String {
    format!(
        "{},{},{},{},{},{},{},{}",
        node_id.as_u32(),
        record.pg_id.get(),
        pg_state_as_str(record.state),
        record.observed_epoch.get(),
        record.observed_at_ms,
        record.metadata_proof.applied_log_index,
        record.metadata_proof.applied_log_hash,
        record.metadata_proof.state_digest
    )
}

fn parse_snapshot(contents: &str) -> Result<ClusterControlSnapshot, ControlPlaneError> {
    let mut version = None;
    let mut authority_incarnation = None;
    let mut cluster_epoch = None;
    let mut nodes = BTreeMap::new();
    let mut pgs = BTreeMap::new();
    let mut pg_lines = BTreeMap::new();
    let mut node_pg_lines = BTreeMap::<(NodeId, PgId), usize>::new();
    let mut history = BTreeMap::<ClusterEpoch, ParsedHistoryRecord>::new();

    for (idx, line) in contents.lines().enumerate() {
        let line_number = idx + 1;
        if line.is_empty() {
            continue;
        }
        if let Some(value) = line.strip_prefix("version=") {
            version = Some(parse_u64(line_number, value, "version")?);
        } else if let Some(value) = line.strip_prefix("authority_incarnation=") {
            authority_incarnation = Some(
                AuthorityIncarnation::new(parse_u64(line_number, value, "authority_incarnation")?)
                    .ok_or_else(|| {
                        parse_error(line_number, "authority incarnation must be nonzero")
                    })?,
            );
        } else if let Some(value) = line.strip_prefix("cluster_epoch=") {
            cluster_epoch = Some(
                ClusterEpoch::new(parse_u64(line_number, value, "cluster_epoch")?)
                    .ok_or_else(|| parse_error(line_number, "cluster epoch must be nonzero"))?,
            );
        } else if let Some(value) = line.strip_prefix("history=") {
            if version == Some(2) {
                return Err(parse_error(
                    line_number,
                    "history records require control-plane state version 3",
                ));
            }
            let record = parse_history_record(line_number, value)?;
            if history
                .insert(
                    record.cluster_epoch,
                    ParsedHistoryRecord::new(record, line_number),
                )
                .is_some()
            {
                return Err(parse_error(line_number, "duplicate history record"));
            }
        } else if let Some(value) = line.strip_prefix("history_node=") {
            if version == Some(2) {
                return Err(parse_error(
                    line_number,
                    "history records require control-plane state version 3",
                ));
            }
            let (epoch, record) = parse_history_node_record(line_number, value)?;
            let history_record = history
                .get_mut(&epoch)
                .ok_or_else(|| parse_error(line_number, "history node references unknown epoch"))?;
            if history_record.node_ids.insert(record.node_id) {
                history_record.record.nodes.push(record);
            } else {
                return Err(parse_error(line_number, "duplicate history node record"));
            }
        } else if let Some(value) = line.strip_prefix("history_node_pg=") {
            let state_version = version.ok_or_else(|| {
                parse_error(line_number, "version must precede PG observation records")
            })?;
            if state_version != 7 {
                return Err(parse_error(
                    line_number,
                    "control-plane state version 7 required",
                ));
            }
            let (epoch, node_id, observation) = parse_history_node_pg_record(line_number, value)?;
            let history_record = history.get_mut(&epoch).ok_or_else(|| {
                parse_error(line_number, "history node PG references unknown epoch")
            })?;
            let node = history_record
                .record
                .nodes
                .iter_mut()
                .find(|record| record.node_id == node_id)
                .ok_or_else(|| {
                    parse_error(line_number, "history node PG references unknown node")
                })?;
            if node
                .pg_observations
                .insert(observation.pg_id, observation)
                .is_some()
            {
                return Err(parse_error(
                    line_number,
                    "duplicate history node PG observation",
                ));
            }
            history_record
                .node_pg_lines
                .insert((node_id, observation.pg_id), line_number);
        } else if let Some(value) = line.strip_prefix("history_pg=") {
            if version == Some(2) {
                return Err(parse_error(
                    line_number,
                    "history records require control-plane state version 3",
                ));
            }
            let state_version = version.ok_or_else(|| {
                parse_error(line_number, "version must precede history PG records")
            })?;
            if state_version != 7 {
                return Err(parse_error(
                    line_number,
                    "control-plane state version 7 required",
                ));
            }
            let (epoch, record) = parse_history_pg_record(line_number, value)?;
            let history_record = history
                .get_mut(&epoch)
                .ok_or_else(|| parse_error(line_number, "history PG references unknown epoch"))?;
            if history_record.pg_ids.insert(record.pg_id) {
                history_record.pg_lines.insert(record.pg_id, line_number);
                history_record.record.pgs.push(record);
            } else {
                return Err(parse_error(line_number, "duplicate history PG record"));
            }
        } else if let Some(value) = line.strip_prefix("node=") {
            let record = parse_node_record(line_number, value)?;
            if nodes.insert(record.node_id, record).is_some() {
                return Err(parse_error(line_number, "duplicate node record"));
            }
        } else if let Some(value) = line.strip_prefix("node_pg=") {
            let state_version = version.ok_or_else(|| {
                parse_error(line_number, "version must precede PG observation records")
            })?;
            if state_version != 7 {
                return Err(parse_error(
                    line_number,
                    "control-plane state version 7 required",
                ));
            }
            let (node_id, observation) = parse_node_pg_record(line_number, value)?;
            let node = nodes
                .get_mut(&node_id)
                .ok_or_else(|| parse_error(line_number, "node PG references unknown node"))?;
            if node
                .pg_observations
                .insert(observation.pg_id, observation)
                .is_some()
            {
                return Err(parse_error(line_number, "duplicate node PG observation"));
            }
            node_pg_lines.insert((node_id, observation.pg_id), line_number);
        } else if let Some(value) = line.strip_prefix("pg=") {
            let state_version = version
                .ok_or_else(|| parse_error(line_number, "version must precede PG records"))?;
            if state_version != 7 {
                return Err(parse_error(
                    line_number,
                    "control-plane state version 7 required",
                ));
            }
            let record = parse_pg_record(line_number, value)?;
            let pg_id = record.pg_id;
            if pgs.insert(pg_id, record).is_some() {
                return Err(parse_error(line_number, "duplicate PG record"));
            }
            pg_lines.insert(pg_id, line_number);
        } else {
            return Err(parse_error(line_number, "unknown control-plane state line"));
        }
    }

    let version = version
        .ok_or_else(|| parse_error(0, "missing or unsupported control-plane state version"))?;
    if version != 7 {
        return Err(parse_error(
            0,
            "missing or unsupported control-plane state version",
        ));
    }
    let cluster_epoch = cluster_epoch.ok_or_else(|| parse_error(0, "missing cluster epoch"))?;
    validate_current_pgs(&pgs, &pg_lines, &nodes)?;
    validate_current_pg_observations(&nodes, &node_pg_lines, &pgs, cluster_epoch)?;
    validate_parsed_history(&history, cluster_epoch)?;
    let mut history: Vec<ClusterMapHistoryRecord> =
        history.into_values().map(|record| record.record).collect();
    prune_cluster_map_history(&mut history);
    Ok(ClusterControlSnapshot {
        authority_incarnation: authority_incarnation
            .ok_or_else(|| parse_error(0, "missing authority incarnation"))?,
        cluster_epoch,
        nodes,
        pgs,
        history,
    })
}

struct ParsedHistoryRecord {
    record: ClusterMapHistoryRecord,
    line: usize,
    node_ids: BTreeSet<NodeId>,
    pg_ids: BTreeSet<PgId>,
    pg_lines: BTreeMap<PgId, usize>,
    node_pg_lines: BTreeMap<(NodeId, PgId), usize>,
}

impl ParsedHistoryRecord {
    fn new(record: ClusterMapHistoryRecord, line: usize) -> Self {
        Self {
            record,
            line,
            node_ids: BTreeSet::new(),
            pg_ids: BTreeSet::new(),
            pg_lines: BTreeMap::new(),
            node_pg_lines: BTreeMap::new(),
        }
    }
}

fn validate_current_pgs(
    pgs: &BTreeMap<PgId, PgControlRecord>,
    pg_lines: &BTreeMap<PgId, usize>,
    nodes: &BTreeMap<NodeId, NodeControlRecord>,
) -> Result<(), ControlPlaneError> {
    for pg in pgs.values() {
        for node_id in &pg.acting_set {
            if !nodes.contains_key(node_id) {
                return Err(parse_error(
                    pg_lines.get(&pg.pg_id).copied().unwrap_or(0),
                    "PG acting set references unknown node",
                ));
            }
        }
    }
    Ok(())
}

fn validate_current_pg_observations(
    nodes: &BTreeMap<NodeId, NodeControlRecord>,
    node_pg_lines: &BTreeMap<(NodeId, PgId), usize>,
    pgs: &BTreeMap<PgId, PgControlRecord>,
    current_epoch: ClusterEpoch,
) -> Result<(), ControlPlaneError> {
    for node in nodes.values() {
        for observation in node.pg_observations.values() {
            let line = node_pg_lines
                .get(&(node.node_id, observation.pg_id))
                .copied()
                .unwrap_or(0);
            if observation.observed_epoch != current_epoch {
                return Err(parse_error(
                    line,
                    "node PG observation epoch must match current cluster epoch",
                ));
            }
            let pg = pgs
                .get(&observation.pg_id)
                .ok_or_else(|| parse_error(line, "node PG observation references unknown PG"))?;
            if !pg.acting_set.contains(&node.node_id) {
                return Err(parse_error(
                    line,
                    "node PG observation references PG outside node acting set",
                ));
            }
            if pg.state == PgState::Active && observation.state == PgState::Active {
                let Some(expected) = pg.active_metadata_proof else {
                    return Err(parse_error(line, "active PG is missing metadata proof"));
                };
                if observation.metadata_proof != expected {
                    return Err(parse_error(
                        line,
                        "active node PG observation metadata proof does not match PG active proof",
                    ));
                }
            }
        }
    }
    Ok(())
}

fn validate_parsed_history(
    history: &BTreeMap<ClusterEpoch, ParsedHistoryRecord>,
    current_epoch: ClusterEpoch,
) -> Result<(), ControlPlaneError> {
    for (epoch, record) in history {
        if *epoch >= current_epoch {
            return Err(parse_error(
                record.line,
                "history epoch must be older than current cluster epoch",
            ));
        }
        for pg in &record.record.pgs {
            for node_id in &pg.acting_set {
                if !record.node_ids.contains(node_id) {
                    return Err(parse_error(
                        record
                            .pg_lines
                            .get(&pg.pg_id)
                            .copied()
                            .unwrap_or(record.line),
                        "history PG acting set references node absent from history map",
                    ));
                }
            }
        }
        for node in &record.record.nodes {
            for observation in node.pg_observations.values() {
                let line = record
                    .node_pg_lines
                    .get(&(node.node_id, observation.pg_id))
                    .copied()
                    .unwrap_or(record.line);
                if observation.observed_epoch != *epoch {
                    return Err(parse_error(
                        line,
                        "history node PG observation epoch must match history epoch",
                    ));
                }
                let pg = record
                    .record
                    .pgs
                    .iter()
                    .find(|pg| pg.pg_id == observation.pg_id)
                    .ok_or_else(|| {
                        parse_error(line, "history node PG observation references unknown PG")
                    })?;
                if !pg.acting_set.contains(&node.node_id) {
                    return Err(parse_error(
                        line,
                        "history node PG observation references PG outside node acting set",
                    ));
                }
            }
        }
    }
    Ok(())
}

fn parse_history_record(
    line: usize,
    value: &str,
) -> Result<ClusterMapHistoryRecord, ControlPlaneError> {
    let fields: Vec<&str> = value.split(',').collect();
    if fields.len() != 2 {
        return Err(parse_error(line, "history record must have two fields"));
    }
    let cluster_epoch = ClusterEpoch::new(parse_u64(line, fields[0], "history cluster epoch")?)
        .ok_or_else(|| parse_error(line, "history cluster epoch must be nonzero"))?;
    let authority_incarnation =
        AuthorityIncarnation::new(parse_u64(line, fields[1], "history authority incarnation")?)
            .ok_or_else(|| parse_error(line, "history authority incarnation must be nonzero"))?;
    Ok(ClusterMapHistoryRecord {
        authority_incarnation,
        cluster_epoch,
        nodes: Vec::new(),
        pgs: Vec::new(),
    })
}

fn parse_history_node_record(
    line: usize,
    value: &str,
) -> Result<(ClusterEpoch, NodeControlRecord), ControlPlaneError> {
    let (epoch, record) = value
        .split_once(',')
        .ok_or_else(|| parse_error(line, "history node record must start with epoch"))?;
    let epoch = ClusterEpoch::new(parse_u64(line, epoch, "history node epoch")?)
        .ok_or_else(|| parse_error(line, "history node epoch must be nonzero"))?;
    Ok((epoch, parse_node_record(line, record)?))
}

fn parse_history_node_pg_record(
    line: usize,
    value: &str,
) -> Result<(ClusterEpoch, NodeId, NodePgObservationRecord), ControlPlaneError> {
    let (epoch, record) = value
        .split_once(',')
        .ok_or_else(|| parse_error(line, "history node PG record must start with epoch"))?;
    let epoch = ClusterEpoch::new(parse_u64(line, epoch, "history node PG epoch")?)
        .ok_or_else(|| parse_error(line, "history node PG epoch must be nonzero"))?;
    let (node_id, record) = parse_node_pg_record(line, record)?;
    Ok((epoch, node_id, record))
}

fn parse_history_pg_record(
    line: usize,
    value: &str,
) -> Result<(ClusterEpoch, PgControlRecord), ControlPlaneError> {
    let (epoch, record) = value
        .split_once(',')
        .ok_or_else(|| parse_error(line, "history PG record must start with epoch"))?;
    let epoch = ClusterEpoch::new(parse_u64(line, epoch, "history PG epoch")?)
        .ok_or_else(|| parse_error(line, "history PG epoch must be nonzero"))?;
    Ok((epoch, parse_pg_record(line, record)?))
}

fn parse_node_pg_record(
    line: usize,
    value: &str,
) -> Result<(NodeId, NodePgObservationRecord), ControlPlaneError> {
    let fields: Vec<&str> = value.split(',').collect();
    if fields.len() != 8 {
        return Err(parse_error(
            line,
            "node PG observation record must have eight fields",
        ));
    }
    let node_id = NodeId::new(parse_u32(line, fields[0], "node id")?);
    let pg_id = PgId::new(parse_u32(line, fields[1], "PG id")?);
    let state = pg_state_from_str(fields[2])?;
    let observed_epoch = ClusterEpoch::new(parse_u64(line, fields[3], "observed epoch")?)
        .ok_or_else(|| parse_error(line, "observed epoch must be nonzero"))?;
    let observed_at_ms = parse_u64(line, fields[4], "observed at")?;
    let metadata_proof = PgMetadataProof {
        applied_log_index: parse_u64(line, fields[5], "applied log index")?,
        applied_log_hash: parse_u64(line, fields[6], "applied log hash")?,
        state_digest: parse_u64(line, fields[7], "state digest")?,
    };
    Ok((
        node_id,
        NodePgObservationRecord {
            pg_id,
            state,
            observed_epoch,
            observed_at_ms,
            metadata_proof,
        },
    ))
}

fn parse_node_record(line: usize, value: &str) -> Result<NodeControlRecord, ControlPlaneError> {
    let fields: Vec<&str> = value.split(',').collect();
    if fields.len() != 8 {
        return Err(parse_error(line, "node record must have eight fields"));
    }
    let node_id = NodeId::new(parse_u32(line, fields[0], "node id")?);
    let membership = NodeMembershipState::from_str(fields[1])?;
    let availability = NodeAvailabilityState::from_str(fields[2])?;
    let node_incarnation = parse_u64(line, fields[3], "node incarnation")?;
    let last_observed_epoch = parse_option_cluster_epoch(line, fields[4], "last observed epoch")?;
    let last_heartbeat_ms = parse_option_u64(line, fields[5], "last heartbeat")?;
    let lease_deadline_ms = parse_option_u64(line, fields[6], "lease deadline")?;
    let endpoint = String::from_utf8(hex_decode(line, fields[7])?)
        .map_err(|_| parse_error(line, "node endpoint must be valid UTF-8 after hex decoding"))?;
    Ok(NodeControlRecord {
        node_id,
        membership,
        availability,
        node_incarnation,
        endpoint,
        last_observed_epoch,
        last_heartbeat_ms,
        lease_deadline_ms,
        pg_observations: BTreeMap::new(),
    })
}

fn parse_pg_record(line: usize, value: &str) -> Result<PgControlRecord, ControlPlaneError> {
    let fields: Vec<&str> = value.split(',').collect();
    if fields.len() != 7 {
        return Err(parse_error(line, "PG record must have seven fields"));
    }
    let pg_id = PgId::new(parse_u32(line, fields[0], "PG id")?);
    let state = pg_state_from_str(fields[1])?;
    let acting_set = parse_node_list(line, fields[2])?;
    let active_primary = parse_option_u32(line, fields[3], "active primary")?.map(NodeId::new);
    let active_log_index = parse_option_u64(line, fields[4], "active applied log index")?;
    let active_log_hash = parse_option_u64(line, fields[5], "active applied log hash")?;
    let active_state_digest = parse_option_u64(line, fields[6], "active state digest")?;
    let active_metadata_proof = match (active_log_index, active_log_hash, active_state_digest) {
        (Some(applied_log_index), Some(applied_log_hash), Some(state_digest)) => {
            Some(PgMetadataProof {
                applied_log_index,
                applied_log_hash,
                state_digest,
            })
        }
        (None, None, None) => None,
        _ => {
            return Err(parse_error(
                line,
                "active PG metadata proof fields must be all present or all absent",
            ));
        }
    };
    if acting_set.is_empty() {
        return Err(parse_error(line, "PG acting set must not be empty"));
    }
    match (state, active_primary, active_metadata_proof) {
        (PgState::Active, None, _) => {
            return Err(parse_error(
                line,
                "active PG record requires active primary",
            ));
        }
        (PgState::Active, Some(_), None) => {
            return Err(parse_error(
                line,
                "active PG record requires active metadata proof",
            ));
        }
        (PgState::Active, Some(primary), Some(_)) if !acting_set.contains(&primary) => {
            return Err(parse_error(line, "active PG primary must be in acting set"));
        }
        (PgState::Active, Some(_), Some(_)) => {}
        (_, Some(_), _) => {
            return Err(parse_error(
                line,
                "non-active PG record must not have active primary",
            ));
        }
        (_, None, Some(_)) => {
            return Err(parse_error(
                line,
                "non-active PG record must not have active metadata proof",
            ));
        }
        (_, None, None) => {}
    }
    let mut unique_nodes = BTreeSet::new();
    for node_id in &acting_set {
        if !unique_nodes.insert(*node_id) {
            return Err(parse_error(line, "PG acting set contains duplicate node"));
        }
    }
    Ok(PgControlRecord {
        pg_id,
        state,
        acting_set,
        active_primary,
        active_metadata_proof,
    })
}

fn option_u64(value: Option<u64>) -> String {
    value.map_or_else(|| "-".to_owned(), |value| value.to_string())
}

fn option_u32(value: Option<u32>) -> String {
    value.map_or_else(|| "-".to_owned(), |value| value.to_string())
}

fn parse_option_u64(
    line: usize,
    value: &str,
    field: &'static str,
) -> Result<Option<u64>, ControlPlaneError> {
    if value == "-" {
        Ok(None)
    } else {
        parse_u64(line, value, field).map(Some)
    }
}

fn parse_option_u32(
    line: usize,
    value: &str,
    field: &'static str,
) -> Result<Option<u32>, ControlPlaneError> {
    if value == "-" {
        Ok(None)
    } else {
        parse_u32(line, value, field).map(Some)
    }
}

fn parse_option_cluster_epoch(
    line: usize,
    value: &str,
    field: &'static str,
) -> Result<Option<ClusterEpoch>, ControlPlaneError> {
    parse_option_u64(line, value, field).and_then(|epoch| {
        epoch
            .map(|epoch| {
                ClusterEpoch::new(epoch)
                    .ok_or_else(|| parse_error(line, "cluster epoch must be nonzero"))
            })
            .transpose()
    })
}

fn validate_acting_set(
    snapshot: &ClusterControlSnapshot,
    pg_id: PgId,
    acting_set: &[NodeId],
) -> Result<(), ControlPlaneError> {
    if acting_set.is_empty() {
        return Err(ControlPlaneError::EmptyActingSet { pg_id: pg_id.get() });
    }
    let mut unique_nodes = BTreeSet::new();
    for node_id in acting_set {
        if !unique_nodes.insert(*node_id) {
            return Err(ControlPlaneError::DuplicateActingSetNode {
                pg_id: pg_id.get(),
                node_id: node_id.as_u32(),
            });
        }
        if !snapshot.nodes.contains_key(node_id) {
            return Err(ControlPlaneError::UnknownActingSetNode {
                pg_id: pg_id.get(),
                node_id: node_id.as_u32(),
            });
        }
    }
    Ok(())
}

fn validate_pg_heartbeat_observations(
    snapshot: &ClusterControlSnapshot,
    node_id: NodeId,
    observations: &[NodePgHeartbeatObservation],
) -> Result<(), ControlPlaneError> {
    let mut observed_pgs = BTreeSet::new();
    for observation in observations {
        if !observed_pgs.insert(observation.pg_id) {
            return Err(ControlPlaneError::DuplicatePgObservation {
                node_id: node_id.as_u32(),
                pg_id: observation.pg_id.get(),
            });
        }
        let pg = snapshot
            .pgs
            .get(&observation.pg_id)
            .ok_or(ControlPlaneError::UnknownPg {
                pg_id: observation.pg_id.get(),
            })?;
        if !pg.acting_set.contains(&node_id) {
            return Err(ControlPlaneError::PgObservationNotInActingSet {
                node_id: node_id.as_u32(),
                pg_id: observation.pg_id.get(),
            });
        }
        if pg.state == PgState::Active && observation.state == PgState::Active {
            let expected = pg.active_metadata_proof.ok_or(
                ControlPlaneError::ActivePgMissingMetadataProof {
                    pg_id: observation.pg_id.get(),
                },
            )?;
            if observation.metadata_proof != expected {
                return Err(ControlPlaneError::PgActiveMetadataProofMismatch {
                    pg_id: observation.pg_id.get(),
                    node_id: node_id.as_u32(),
                    cluster_epoch: snapshot.cluster_epoch,
                    expected,
                    actual: observation.metadata_proof,
                });
            }
        }
    }
    Ok(())
}

fn validate_pg_peering_observations(
    snapshot: &ClusterControlSnapshot,
    pg_id: PgId,
    acting_set: &[NodeId],
    now_ms: u64,
) -> Result<PgMetadataProof, ControlPlaneError> {
    let mut expected_proof = None;
    for node_id in acting_set {
        let Some(node) = snapshot.nodes.get(node_id) else {
            return Err(ControlPlaneError::UnknownActingSetNode {
                pg_id: pg_id.get(),
                node_id: node_id.as_u32(),
            });
        };
        if !node.can_serve_primary(snapshot.cluster_epoch, now_ms) {
            continue;
        }
        let observation =
            node.pg_observation(pg_id)
                .ok_or(ControlPlaneError::PgPeeringMissingObservation {
                    pg_id: pg_id.get(),
                    node_id: node_id.as_u32(),
                    cluster_epoch: snapshot.cluster_epoch,
                })?;
        if observation.observed_epoch != snapshot.cluster_epoch {
            return Err(ControlPlaneError::StaleNodeObservedEpoch {
                node_id: node_id.as_u32(),
                observed_epoch: observation.observed_epoch,
                current_epoch: snapshot.cluster_epoch,
            });
        }
        if observation.state != PgState::Peering {
            return Err(ControlPlaneError::PgPeeringObservationNotPeering {
                pg_id: pg_id.get(),
                node_id: node_id.as_u32(),
                cluster_epoch: snapshot.cluster_epoch,
                state: observation.state,
            });
        }
        match expected_proof {
            Some(expected) if observation.metadata_proof != expected => {
                return Err(ControlPlaneError::PgPeeringMetadataProofMismatch {
                    pg_id: pg_id.get(),
                    node_id: node_id.as_u32(),
                    cluster_epoch: snapshot.cluster_epoch,
                    expected,
                    actual: observation.metadata_proof,
                });
            }
            Some(_) => {}
            None => expected_proof = Some(observation.metadata_proof),
        }
    }
    expected_proof.ok_or(ControlPlaneError::PgPeeringMissingObservation {
        pg_id: pg_id.get(),
        node_id: 0,
        cluster_epoch: snapshot.cluster_epoch,
    })
}

fn primary_has_current_pg_state(
    snapshot: &ClusterControlSnapshot,
    pg_id: PgId,
    primary: NodeId,
    expected_state: PgState,
) -> bool {
    let expected_active_proof = snapshot
        .pgs
        .get(&pg_id)
        .and_then(|pg| pg.active_metadata_proof);
    snapshot
        .nodes
        .get(&primary)
        .and_then(|node| node.pg_observation(pg_id))
        .is_some_and(|observation| {
            observation.observed_epoch == snapshot.cluster_epoch
                && observation.state == expected_state
                && (expected_state != PgState::Active
                    || Some(observation.metadata_proof) == expected_active_proof)
        })
}

fn validate_pg_primary_active_observation(
    snapshot: &ClusterControlSnapshot,
    pg_id: PgId,
    primary: NodeId,
) -> Result<(), ControlPlaneError> {
    let pg = snapshot
        .pgs
        .get(&pg_id)
        .ok_or(ControlPlaneError::UnknownPg { pg_id: pg_id.get() })?;
    let expected_proof = pg
        .active_metadata_proof
        .ok_or(ControlPlaneError::ActivePgMissingMetadataProof { pg_id: pg_id.get() })?;
    let Some(node) = snapshot.nodes.get(&primary) else {
        return Err(ControlPlaneError::UnknownNode {
            node_id: primary.as_u32(),
        });
    };
    let observation =
        node.pg_observation(pg_id)
            .ok_or(ControlPlaneError::PgPrimaryMissingActiveObservation {
                pg_id: pg_id.get(),
                node_id: primary.as_u32(),
                cluster_epoch: snapshot.cluster_epoch,
            })?;
    if observation.observed_epoch != snapshot.cluster_epoch {
        return Err(ControlPlaneError::StaleNodeObservedEpoch {
            node_id: primary.as_u32(),
            observed_epoch: observation.observed_epoch,
            current_epoch: snapshot.cluster_epoch,
        });
    }
    if observation.state != PgState::Active {
        return Err(ControlPlaneError::PgPrimaryObservationNotActive {
            pg_id: pg_id.get(),
            node_id: primary.as_u32(),
            cluster_epoch: snapshot.cluster_epoch,
            state: observation.state,
        });
    }
    if observation.metadata_proof != expected_proof {
        return Err(ControlPlaneError::PgActiveMetadataProofMismatch {
            pg_id: pg_id.get(),
            node_id: primary.as_u32(),
            cluster_epoch: snapshot.cluster_epoch,
            expected: expected_proof,
            actual: observation.metadata_proof,
        });
    }
    Ok(())
}

fn prune_cluster_map_history(history: &mut Vec<ClusterMapHistoryRecord>) {
    history.sort_by_key(ClusterMapHistoryRecord::cluster_epoch);
    let excess = history.len().saturating_sub(CLUSTER_MAP_HISTORY_LIMIT);
    if excess > 0 {
        history.drain(..excess);
    }
}

fn mark_pgs_peering_for_nodes(
    snapshot: &mut ClusterControlSnapshot,
    nodes: impl IntoIterator<Item = NodeId>,
) -> Vec<PgId> {
    let affected_nodes: Vec<NodeId> = nodes.into_iter().collect();
    let mut peering_pgs = Vec::new();
    for record in snapshot.pgs.values_mut() {
        if record.state != PgState::Peering
            && record
                .acting_set
                .iter()
                .any(|node_id| affected_nodes.contains(node_id))
        {
            record.state = PgState::Peering;
            record.active_primary = None;
            record.active_metadata_proof = None;
            peering_pgs.push(record.pg_id);
        }
    }
    peering_pgs
}

fn pg_state_as_str(state: PgState) -> &'static str {
    match state {
        PgState::Active => "active",
        PgState::Peering => "peering",
        PgState::Degraded => "degraded",
        PgState::Backfilling => "backfilling",
        PgState::Inconsistent => "inconsistent",
    }
}

fn pg_state_from_str(value: &str) -> Result<PgState, ControlPlaneError> {
    match value {
        "active" => Ok(PgState::Active),
        "peering" => Ok(PgState::Peering),
        "degraded" => Ok(PgState::Degraded),
        "backfilling" => Ok(PgState::Backfilling),
        "inconsistent" => Ok(PgState::Inconsistent),
        _ => Err(ControlPlaneError::InvalidState {
            field: "pg",
            value: value.to_owned(),
        }),
    }
}

fn format_node_list(nodes: &[NodeId]) -> String {
    nodes
        .iter()
        .map(|node_id| node_id.as_u32().to_string())
        .collect::<Vec<_>>()
        .join(":")
}

fn parse_node_list(line: usize, value: &str) -> Result<Vec<NodeId>, ControlPlaneError> {
    if value.is_empty() {
        return Ok(Vec::new());
    }
    value
        .split(':')
        .map(|value| parse_u32(line, value, "node id").map(NodeId::new))
        .collect()
}

fn parse_u32(line: usize, value: &str, field: &'static str) -> Result<u32, ControlPlaneError> {
    value
        .parse::<u32>()
        .map_err(|source| parse_error(line, &format!("invalid {field} {value:?}: {source}")))
}

fn parse_u64(line: usize, value: &str, field: &'static str) -> Result<u64, ControlPlaneError> {
    value
        .parse::<u64>()
        .map_err(|source| parse_error(line, &format!("invalid {field} {value:?}: {source}")))
}

fn parse_error(line: usize, message: &str) -> ControlPlaneError {
    ControlPlaneError::Parse {
        line,
        message: message.to_owned(),
    }
}

fn hex_encode(bytes: &[u8]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut out = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        out.push(HEX[(byte >> 4) as usize] as char);
        out.push(HEX[(byte & 0x0f) as usize] as char);
    }
    out
}

fn hex_decode(line: usize, value: &str) -> Result<Vec<u8>, ControlPlaneError> {
    if !value.len().is_multiple_of(2) {
        return Err(parse_error(line, "hex string has odd length"));
    }
    let mut out = Vec::with_capacity(value.len() / 2);
    for pair in value.as_bytes().chunks_exact(2) {
        let high = hex_nibble(pair[0]).ok_or_else(|| parse_error(line, "invalid hex digit"))?;
        let low = hex_nibble(pair[1]).ok_or_else(|| parse_error(line, "invalid hex digit"))?;
        out.push((high << 4) | low);
    }
    Ok(out)
}

fn hex_nibble(byte: u8) -> Option<u8> {
    match byte {
        b'0'..=b'9' => Some(byte - b'0'),
        b'a'..=b'f' => Some(byte - b'a' + 10),
        b'A'..=b'F' => Some(byte - b'A' + 10),
        _ => None,
    }
}

fn state_parent(path: &Path) -> Option<&Path> {
    path.parent()
        .filter(|parent| !parent.as_os_str().is_empty())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::cell::Cell;
    use std::sync::{
        atomic::{AtomicU64, Ordering},
        Arc,
    };
    use std::time::{Duration, Instant};

    #[derive(Debug)]
    struct FailingStore {
        snapshot: ClusterControlSnapshot,
        fail_saves: Cell<bool>,
    }

    impl FailingStore {
        fn new(snapshot: ClusterControlSnapshot) -> Self {
            Self {
                snapshot,
                fail_saves: Cell::new(false),
            }
        }

        fn fail_saves(&self) {
            self.fail_saves.set(true);
        }
    }

    impl ControlPlaneStore for FailingStore {
        fn load(&self) -> Result<Option<ClusterControlSnapshot>, ControlPlaneError> {
            Ok(Some(self.snapshot.clone()))
        }

        fn save(&self, _snapshot: &ClusterControlSnapshot) -> Result<(), ControlPlaneError> {
            if self.fail_saves.get() {
                Err(ControlPlaneError::Io {
                    context: "test save failure",
                    source: std::io::Error::other("injected save failure"),
                })
            } else {
                Ok(())
            }
        }
    }

    fn heartbeat(node_id: u32, observed_epoch: ClusterEpoch, _now_ms: u64) -> NodeHeartbeat {
        NodeHeartbeat {
            node_id: NodeId::new(node_id),
            node_incarnation: 10 + u64::from(node_id),
            endpoint: format!("node-{node_id}.sock"),
            observed_epoch,
            requested_lease_duration_ms: 100,
            pg_observations: Vec::new(),
        }
    }

    fn heartbeat_until_serving(
        authority: &mut SingleAuthorityControlPlane<FileControlPlaneStore>,
        node_id: u32,
        now_ms: u64,
    ) -> HeartbeatLease {
        let first = authority
            .heartbeat(
                heartbeat(node_id, authority.snapshot().cluster_epoch(), now_ms),
                now_ms,
            )
            .unwrap();
        if first.serving() {
            first
        } else {
            authority
                .heartbeat(
                    heartbeat_from_record(authority, node_id, first.cluster_epoch(), now_ms + 1),
                    now_ms + 1,
                )
                .unwrap()
        }
    }

    fn heartbeat_until_serving_with_endpoint(
        authority: &mut SingleAuthorityControlPlane<FileControlPlaneStore>,
        node_id: u32,
        now_ms: u64,
        endpoint: String,
    ) -> HeartbeatLease {
        let mut heartbeat = heartbeat(node_id, authority.snapshot().cluster_epoch(), now_ms);
        heartbeat.endpoint = endpoint;
        let first = authority.heartbeat(heartbeat, now_ms).unwrap();
        if first.serving() {
            first
        } else {
            authority
                .heartbeat(
                    heartbeat_from_record(authority, node_id, first.cluster_epoch(), now_ms + 1),
                    now_ms + 1,
                )
                .unwrap()
        }
    }

    fn heartbeat_from_record<S: ControlPlaneStore>(
        authority: &SingleAuthorityControlPlane<S>,
        node_id: u32,
        observed_epoch: ClusterEpoch,
        _now_ms: u64,
    ) -> NodeHeartbeat {
        let record = authority.snapshot().node(NodeId::new(node_id)).unwrap();
        let mut heartbeat = heartbeat(node_id, observed_epoch, _now_ms);
        heartbeat.node_incarnation = record.node_incarnation();
        heartbeat.endpoint = record.endpoint().to_owned();
        heartbeat
    }

    fn heartbeat_with_pg_observation<S: ControlPlaneStore>(
        authority: &mut SingleAuthorityControlPlane<S>,
        node_id: u32,
        pg_id: u32,
        state: PgState,
        now_ms: u64,
    ) -> HeartbeatLease {
        let mut heartbeat = heartbeat_from_record(
            authority,
            node_id,
            authority.snapshot().cluster_epoch(),
            now_ms,
        );
        heartbeat.pg_observations = vec![NodePgHeartbeatObservation {
            pg_id: PgId::new(pg_id),
            state,
            metadata_proof: PgMetadataProof::empty(),
        }];
        authority.heartbeat(heartbeat, now_ms).unwrap()
    }

    fn node_incarnation<S: ControlPlaneStore>(
        authority: &SingleAuthorityControlPlane<S>,
        node_id: u32,
    ) -> u64 {
        authority
            .snapshot()
            .node(NodeId::new(node_id))
            .unwrap()
            .node_incarnation()
    }

    #[test]
    fn unix_control_plane_client_fetches_runtime_map() {
        let tmp = test_util::tempdir();
        let socket_path = tmp.path().join("control-plane.sock");
        let store = FileControlPlaneStore::new(tmp.path().join("control-plane.state"));
        let mut authority = SingleAuthorityControlPlane::open(store).unwrap();
        authority
            .set_node_membership(NodeId::new(1), NodeMembershipState::Active)
            .unwrap();
        authority
            .set_pg_acting_set(PgId::new(7), vec![NodeId::new(1)])
            .unwrap();
        heartbeat_until_serving_with_endpoint(
            &mut authority,
            1,
            1_000,
            "/tmp/argmin-node-1.sock".to_owned(),
        );
        let expected_epoch = authority.snapshot().cluster_epoch();
        let listener = std::os::unix::net::UnixListener::bind(&socket_path).unwrap();
        let server = std::thread::spawn(move || {
            let (mut stream, _addr) = listener.accept().unwrap();
            handle_control_plane_unix_stream(&mut authority, &mut stream, 1_001).unwrap();
        });

        let client = UnixControlPlaneClient::new(&socket_path);
        let runtime_map = client.runtime_map_snapshot(0).unwrap();

        server.join().unwrap();
        assert_eq!(runtime_map.cluster_epoch(), expected_epoch);
        assert_eq!(runtime_map.nodes().len(), 1);
        assert_eq!(runtime_map.nodes()[0].node_id(), NodeId::new(1));
        assert_eq!(runtime_map.nodes()[0].endpoint(), "/tmp/argmin-node-1.sock");
        assert_eq!(runtime_map.pg_routes().len(), 1);
        assert_eq!(runtime_map.pg_routes()[0].pg_id(), PgId::new(7));
        assert_eq!(runtime_map.pg_routes()[0].state(), PgState::Peering);
    }

    #[test]
    fn unix_control_plane_client_refreshes_heartbeat_and_runtime_map_together() {
        let tmp = test_util::tempdir();
        let socket_path = tmp.path().join("control-plane.sock");
        let store = FileControlPlaneStore::new(tmp.path().join("control-plane.state"));
        let mut authority = SingleAuthorityControlPlane::open(store).unwrap();
        authority
            .set_node_membership(NodeId::new(1), NodeMembershipState::Active)
            .unwrap();
        authority
            .set_pg_acting_set(PgId::new(7), vec![NodeId::new(1)])
            .unwrap();
        let heartbeat_epoch = authority.snapshot().cluster_epoch();
        let listener = std::os::unix::net::UnixListener::bind(&socket_path).unwrap();
        let server = std::thread::spawn(move || {
            let (mut stream, _addr) = listener.accept().unwrap();
            handle_control_plane_unix_stream(&mut authority, &mut stream, 2_000).unwrap();
        });

        let mut client = UnixControlPlaneClient::new(&socket_path);
        let refresh = client
            .refresh_node_heartbeat(
                NodeHeartbeat {
                    node_id: NodeId::new(1),
                    node_incarnation: 42,
                    endpoint: "/tmp/argmin-node-1.sock".to_owned(),
                    observed_epoch: heartbeat_epoch,
                    requested_lease_duration_ms: 100,
                    pg_observations: Vec::new(),
                },
                0,
            )
            .unwrap();

        server.join().unwrap();
        assert_eq!(refresh.lease().node_id(), NodeId::new(1));
        assert_eq!(refresh.lease().lease_deadline_ms(), 2_100);
        assert!(!refresh.lease().serving());
        assert_eq!(
            refresh.lease().cluster_epoch(),
            refresh.runtime_map().cluster_epoch()
        );
        assert_eq!(refresh.runtime_map().nodes().len(), 1);
        assert_eq!(
            refresh.runtime_map().nodes()[0].endpoint(),
            "/tmp/argmin-node-1.sock"
        );
    }

    #[test]
    fn unix_control_plane_client_receives_framed_authority_error() {
        let tmp = test_util::tempdir();
        let socket_path = tmp.path().join("control-plane.sock");
        let store = FileControlPlaneStore::new(tmp.path().join("control-plane.state"));
        let mut authority = SingleAuthorityControlPlane::open(store).unwrap();
        let listener = std::os::unix::net::UnixListener::bind(&socket_path).unwrap();
        let server = std::thread::spawn(move || {
            let (mut stream, _addr) = listener.accept().unwrap();
            handle_control_plane_unix_stream(&mut authority, &mut stream, 2_000).unwrap();
        });

        let mut client = UnixControlPlaneClient::new(&socket_path);
        let error = client
            .refresh_node_heartbeat(
                NodeHeartbeat {
                    node_id: NodeId::new(99),
                    node_incarnation: 1,
                    endpoint: "/tmp/argmin-node-99.sock".to_owned(),
                    observed_epoch: ClusterEpoch::INITIAL,
                    requested_lease_duration_ms: 100,
                    pg_observations: Vec::new(),
                },
                0,
            )
            .unwrap_err();

        server.join().unwrap();
        assert!(matches!(
            error,
            ControlPlaneError::RpcRemote { message } if message.contains("unknown node 99")
        ));
    }

    #[test]
    fn control_plane_rpc_rejects_corrupted_payload_checksum() {
        let (mut writer, mut reader) = UnixStream::pair().unwrap();
        let payload = b"not a valid request";
        writer.write_all(CONTROL_PLANE_RPC_MAGIC).unwrap();
        write_u16_to_stream(&mut writer, 1);
        write_u16_to_stream(
            &mut writer,
            ControlPlaneRpcKind::RefreshNodeHeartbeat as u16,
        );
        write_u32_to_stream(&mut writer, payload.len() as u32);
        write_u64_to_stream(&mut writer, 0);
        writer.write_all(payload).unwrap();

        let error = read_control_plane_unix_request(&mut reader).unwrap_err();

        assert!(matches!(
            error,
            ControlPlaneError::RpcProtocol { message }
                if message.contains("checksum mismatch")
        ));
    }

    #[test]
    fn control_plane_rpc_rejects_corrupted_header_checksum() {
        let (mut writer, mut reader) = UnixStream::pair().unwrap();
        let payload = b"";
        let checksum = control_plane_rpc_frame_checksum(
            1,
            ControlPlaneRpcKind::RuntimeMapSnapshot as u16,
            payload.len() as u32,
            payload,
        );
        writer.write_all(CONTROL_PLANE_RPC_MAGIC).unwrap();
        write_u16_to_stream(&mut writer, 1);
        write_u16_to_stream(
            &mut writer,
            ControlPlaneRpcKind::RefreshNodeHeartbeat as u16,
        );
        write_u32_to_stream(&mut writer, payload.len() as u32);
        write_u64_to_stream(&mut writer, checksum);

        let error = read_control_plane_unix_request(&mut reader).unwrap_err();

        assert!(matches!(
            error,
            ControlPlaneError::RpcProtocol { message }
                if message.contains("checksum mismatch")
        ));
    }

    #[test]
    fn unix_control_plane_client_rejects_corrupted_response_checksum() {
        let tmp = test_util::tempdir();
        let socket_path = tmp.path().join("control-plane.sock");
        let listener = std::os::unix::net::UnixListener::bind(&socket_path).unwrap();
        let server = std::thread::spawn(move || {
            let (mut stream, _addr) = listener.accept().unwrap();
            let _request = read_control_plane_unix_request(&mut stream).unwrap();
            let payload = [0u8];
            stream.write_all(CONTROL_PLANE_RPC_MAGIC).unwrap();
            write_u16_to_stream(&mut stream, 1);
            write_u16_to_stream(&mut stream, ControlPlaneRpcKind::RuntimeMapSnapshot as u16);
            write_u32_to_stream(&mut stream, payload.len() as u32);
            write_u64_to_stream(&mut stream, 0);
            stream.write_all(&payload).unwrap();
        });

        let client = UnixControlPlaneClient::new(&socket_path);
        let error = client.runtime_map_snapshot(0).unwrap_err();

        server.join().unwrap();
        assert!(matches!(
            error,
            ControlPlaneError::RpcProtocol { message }
                if message.contains("checksum mismatch")
        ));
    }

    #[test]
    fn control_plane_rpc_rejects_oversized_heartbeat_observation_count_before_allocation() {
        let mut payload = Vec::new();
        write_u32(&mut payload, 1);
        write_u64(&mut payload, 1);
        write_string(&mut payload, "/tmp/argmin-node-1.sock").unwrap();
        write_u64(&mut payload, ClusterEpoch::INITIAL.get());
        write_u64(&mut payload, 100);
        write_u32(&mut payload, u32::MAX);

        let mut reader = PayloadReader::new(&payload);
        let error = read_node_heartbeat(&mut reader).unwrap_err();

        assert!(matches!(
            error,
            ControlPlaneError::RpcProtocol { message }
                if message.contains("PG observations count")
        ));
    }

    #[test]
    fn control_plane_rpc_rejects_oversized_runtime_node_count_before_allocation() {
        let mut payload = Vec::new();
        write_u64(&mut payload, ClusterEpoch::INITIAL.get());
        write_option_u64(&mut payload, None);
        write_u32(&mut payload, u32::MAX);

        let mut reader = PayloadReader::new(&payload);
        let error = read_runtime_map_snapshot(&mut reader).unwrap_err();

        assert!(matches!(
            error,
            ControlPlaneError::RpcProtocol { message }
                if message.contains("runtime nodes count")
        ));
    }

    #[test]
    fn control_plane_rpc_rejects_oversized_runtime_route_count_before_allocation() {
        let mut payload = Vec::new();
        write_u64(&mut payload, ClusterEpoch::INITIAL.get());
        write_option_u64(&mut payload, None);
        write_u32(&mut payload, 0);
        write_u32(&mut payload, u32::MAX);

        let mut reader = PayloadReader::new(&payload);
        let error = read_runtime_map_snapshot(&mut reader).unwrap_err();

        assert!(matches!(
            error,
            ControlPlaneError::RpcProtocol { message }
                if message.contains("PG routes count")
        ));
    }

    #[test]
    fn control_plane_rpc_rejects_oversized_runtime_acting_set_count_before_allocation() {
        let mut payload = Vec::new();
        write_u64(&mut payload, ClusterEpoch::INITIAL.get());
        write_option_u64(&mut payload, None);
        write_u32(&mut payload, 0);
        write_u32(&mut payload, 1);
        write_u64(&mut payload, ClusterEpoch::INITIAL.get());
        write_u32(&mut payload, 7);
        write_u32(&mut payload, 1);
        write_pg_state(&mut payload, PgState::Active);
        write_option_u64(&mut payload, None);
        write_u32(&mut payload, u32::MAX);

        let mut reader = PayloadReader::new(&payload);
        let error = read_runtime_map_snapshot(&mut reader).unwrap_err();

        assert!(matches!(
            error,
            ControlPlaneError::RpcProtocol { message }
                if message.contains("PG route acting set count")
        ));
    }

    fn write_u16_to_stream(stream: &mut UnixStream, value: u16) {
        stream.write_all(&value.to_be_bytes()).unwrap();
    }

    fn write_u32_to_stream(stream: &mut UnixStream, value: u32) {
        stream.write_all(&value.to_be_bytes()).unwrap();
    }

    fn write_u64_to_stream(stream: &mut UnixStream, value: u64) {
        stream.write_all(&value.to_be_bytes()).unwrap();
    }

    #[test]
    fn file_backed_authority_restarts_with_never_reused_epoch_and_incarnation() {
        let tmp = test_util::tempdir();
        let store = FileControlPlaneStore::new(tmp.path().join("control-plane.state"));
        let mut authority = SingleAuthorityControlPlane::open(store.clone()).unwrap();
        assert_eq!(
            authority.snapshot().authority_incarnation(),
            AuthorityIncarnation::INITIAL
        );
        assert_eq!(authority.snapshot().cluster_epoch(), ClusterEpoch::INITIAL);

        authority
            .set_node_membership(NodeId::new(1), NodeMembershipState::Active)
            .unwrap();
        let first_lease = authority
            .heartbeat(
                heartbeat(1, authority.snapshot().cluster_epoch(), 1_000),
                1_000,
            )
            .unwrap();

        let restarted = SingleAuthorityControlPlane::open(store).unwrap();
        assert!(restarted.snapshot().authority_incarnation() > first_lease.authority_incarnation());
        assert!(restarted.snapshot().cluster_epoch() > first_lease.cluster_epoch());
    }

    #[test]
    fn file_backed_authority_restarts_empty_state_with_new_epoch_and_incarnation() {
        let tmp = test_util::tempdir();
        let store = FileControlPlaneStore::new(tmp.path().join("control-plane.state"));
        let authority = SingleAuthorityControlPlane::open(store.clone()).unwrap();
        assert_eq!(
            authority.snapshot().authority_incarnation(),
            AuthorityIncarnation::INITIAL
        );
        assert_eq!(authority.snapshot().cluster_epoch(), ClusterEpoch::INITIAL);

        let restarted = SingleAuthorityControlPlane::open(store).unwrap();
        assert!(restarted.snapshot().authority_incarnation() > AuthorityIncarnation::INITIAL);
        assert!(restarted.snapshot().cluster_epoch() > ClusterEpoch::INITIAL);
        assert_eq!(restarted.snapshot().nodes().count(), 0);
    }

    #[test]
    fn file_backed_authority_rejects_pre_v7_state() {
        let tmp = test_util::tempdir();
        let path = tmp.path().join("control-plane.state");
        std::fs::write(
            &path,
            "version=2\nauthority_incarnation=1\ncluster_epoch=1\nnode=1,active,healthy,11,1,100,200,6e6f64652d312e736f636b\n",
        )
        .unwrap();
        let store = FileControlPlaneStore::new(path);
        assert!(matches!(
            SingleAuthorityControlPlane::open(store),
            Err(ControlPlaneError::Parse { message, .. })
                if message == "missing or unsupported control-plane state version"
        ));
    }

    #[test]
    fn cluster_map_history_is_persisted_across_epoch_changes_and_pruned() {
        let tmp = test_util::tempdir();
        let store = FileControlPlaneStore::new(tmp.path().join("control-plane.state"));
        let mut authority = SingleAuthorityControlPlane::open(store.clone()).unwrap();
        let initial_epoch = authority.snapshot().cluster_epoch();
        authority
            .set_node_membership(NodeId::new(1), NodeMembershipState::Active)
            .unwrap();

        let persisted = store.load().unwrap().unwrap();
        let initial_history = persisted.cluster_map_at_epoch(initial_epoch).unwrap();
        assert_eq!(
            initial_history.authority_incarnation(),
            AuthorityIncarnation::INITIAL
        );
        assert_eq!(initial_history.nodes().len(), 0);
        assert_eq!(initial_history.pgs().len(), 0);

        authority
            .set_node_membership(NodeId::new(2), NodeMembershipState::Active)
            .unwrap();
        authority
            .set_pg_acting_set(PgId::new(3), vec![NodeId::new(1), NodeId::new(2)])
            .unwrap();
        let pg_epoch = authority.snapshot().cluster_epoch();
        let restarted = SingleAuthorityControlPlane::open(store.clone()).unwrap();
        let before_restart = restarted.snapshot().cluster_map_at_epoch(pg_epoch).unwrap();
        assert_eq!(before_restart.pgs().len(), 1);
        assert_eq!(
            before_restart.pgs()[0].acting_set(),
            &[NodeId::new(1), NodeId::new(2)]
        );

        let mut authority = restarted;
        for node_id in 10..(10 + CLUSTER_MAP_HISTORY_LIMIT as u32 + 8) {
            authority
                .set_node_membership(NodeId::new(node_id), NodeMembershipState::Active)
                .unwrap();
        }
        let history = authority.snapshot().cluster_map_history();
        assert_eq!(history.len(), CLUSTER_MAP_HISTORY_LIMIT);
        assert!(history.first().unwrap().cluster_epoch() > initial_epoch);
        assert!(history.last().unwrap().cluster_epoch() < authority.snapshot().cluster_epoch());

        let persisted = store.load().unwrap().unwrap();
        assert_eq!(
            persisted.cluster_map_history().len(),
            CLUSTER_MAP_HISTORY_LIMIT
        );
        assert!(persisted.cluster_map_at_epoch(initial_epoch).is_none());
    }

    #[test]
    fn file_backed_authority_rejects_duplicate_pg_acting_set_nodes() {
        let tmp = test_util::tempdir();
        let path = tmp.path().join("control-plane.state");
        std::fs::write(
            &path,
            "version=7\nauthority_incarnation=1\ncluster_epoch=1\npg=7,peering,1:1,-,-,-,-\n",
        )
        .unwrap();
        let store = FileControlPlaneStore::new(path);
        assert!(matches!(
            SingleAuthorityControlPlane::open(store),
            Err(ControlPlaneError::Parse { message, .. })
                if message == "PG acting set contains duplicate node"
        ));
    }

    #[test]
    fn file_backed_authority_rejects_current_pg_nodes_absent_from_current_map() {
        let tmp = test_util::tempdir();
        let path = tmp.path().join("control-plane.state");
        std::fs::write(
            &path,
            concat!(
                "version=7\n",
                "authority_incarnation=1\n",
                "cluster_epoch=2\n",
                "node=1,active,healthy,11,2,100,200,6e6f64652d312e736f636b\n",
                "pg=7,active,1:99,1,1,2,3\n",
            ),
        )
        .unwrap();
        let store = FileControlPlaneStore::new(path);
        assert!(matches!(
            SingleAuthorityControlPlane::open(store),
            Err(ControlPlaneError::Parse { message, .. })
                if message == "PG acting set references unknown node"
        ));
    }

    #[test]
    fn file_backed_authority_rejects_history_for_unsupported_version() {
        let tmp = test_util::tempdir();
        let path = tmp.path().join("control-plane.state");
        std::fs::write(
            &path,
            "version=5\nauthority_incarnation=1\ncluster_epoch=2\nhistory=1,1\n",
        )
        .unwrap();
        let store = FileControlPlaneStore::new(path);
        assert!(matches!(
            SingleAuthorityControlPlane::open(store),
            Err(ControlPlaneError::Parse { message, .. })
                if message == "missing or unsupported control-plane state version"
        ));
    }

    #[test]
    fn file_backed_authority_rejects_current_or_future_history_epochs() {
        let tmp = test_util::tempdir();
        let path = tmp.path().join("control-plane.state");
        std::fs::write(
            &path,
            "version=7\nauthority_incarnation=1\ncluster_epoch=2\nhistory=2,1\n",
        )
        .unwrap();
        let store = FileControlPlaneStore::new(path);
        assert!(matches!(
            SingleAuthorityControlPlane::open(store),
            Err(ControlPlaneError::Parse { message, .. })
                if message == "history epoch must be older than current cluster epoch"
        ));
    }

    #[test]
    fn file_backed_authority_rejects_history_pg_nodes_absent_from_history_map() {
        let tmp = test_util::tempdir();
        let path = tmp.path().join("control-plane.state");
        std::fs::write(
            &path,
            concat!(
                "version=7\n",
                "authority_incarnation=1\n",
                "cluster_epoch=3\n",
                "history=2,1\n",
                "history_node=2,1,active,healthy,11,2,100,200,6e6f64652d312e736f636b\n",
                "history_pg=2,7,peering,1:2,-,-,-,-\n",
            ),
        )
        .unwrap();
        let store = FileControlPlaneStore::new(path);
        assert!(matches!(
            SingleAuthorityControlPlane::open(store),
            Err(ControlPlaneError::Parse { message, .. })
                if message == "history PG acting set references node absent from history map"
        ));
    }

    #[test]
    fn file_backed_authority_rejects_pg_observations_for_unsupported_version() {
        let tmp = test_util::tempdir();
        let path = tmp.path().join("control-plane.state");
        std::fs::write(
            &path,
            concat!(
                "version=5\n",
                "authority_incarnation=1\n",
                "cluster_epoch=2\n",
                "node=1,active,healthy,11,2,100,200,6e6f64652d312e736f636b\n",
                "node_pg=1,7,peering,2,100,0,0,0\n",
                "pg=7,peering,1,-,-,-,-\n",
            ),
        )
        .unwrap();
        let store = FileControlPlaneStore::new(path);
        assert!(matches!(
            SingleAuthorityControlPlane::open(store),
            Err(ControlPlaneError::Parse { message, .. })
                if message == "control-plane state version 7 required"
        ));
    }

    #[test]
    fn file_backed_authority_rejects_current_pg_observation_outside_acting_set() {
        let tmp = test_util::tempdir();
        let path = tmp.path().join("control-plane.state");
        std::fs::write(
            &path,
            concat!(
                "version=7\n",
                "authority_incarnation=1\n",
                "cluster_epoch=2\n",
                "node=1,active,healthy,11,2,100,200,6e6f64652d312e736f636b\n",
                "node=2,active,healthy,12,2,100,200,6e6f64652d322e736f636b\n",
                "node_pg=2,7,peering,2,100,0,0,0\n",
                "pg=7,peering,1,-,-,-,-\n",
            ),
        )
        .unwrap();
        let store = FileControlPlaneStore::new(path);
        assert!(matches!(
            SingleAuthorityControlPlane::open(store),
            Err(ControlPlaneError::Parse { message, .. })
                if message == "node PG observation references PG outside node acting set"
        ));
    }

    #[test]
    fn file_backed_authority_rejects_current_pg_observation_wrong_epoch() {
        let tmp = test_util::tempdir();
        let path = tmp.path().join("control-plane.state");
        std::fs::write(
            &path,
            concat!(
                "version=7\n",
                "authority_incarnation=1\n",
                "cluster_epoch=3\n",
                "node=1,active,healthy,11,3,100,200,6e6f64652d312e736f636b\n",
                "node_pg=1,7,peering,2,100,0,0,0\n",
                "pg=7,peering,1,-,-,-,-\n",
            ),
        )
        .unwrap();
        let store = FileControlPlaneStore::new(path);
        assert!(matches!(
            SingleAuthorityControlPlane::open(store),
            Err(ControlPlaneError::Parse { message, .. })
                if message == "node PG observation epoch must match current cluster epoch"
        ));
    }

    #[test]
    fn file_backed_authority_rejects_active_pg_observation_with_mismatched_proof() {
        let tmp = test_util::tempdir();
        let path = tmp.path().join("control-plane.state");
        std::fs::write(
            &path,
            concat!(
                "version=7\n",
                "authority_incarnation=1\n",
                "cluster_epoch=2\n",
                "node=1,active,healthy,11,2,100,200,6e6f64652d312e736f636b\n",
                "node_pg=1,7,active,2,100,9,10,12\n",
                "pg=7,active,1,1,9,10,11\n",
            ),
        )
        .unwrap();
        let store = FileControlPlaneStore::new(path);
        assert!(matches!(
            SingleAuthorityControlPlane::open(store),
            Err(ControlPlaneError::Parse { message, .. })
                if message
                    == "active node PG observation metadata proof does not match PG active proof"
        ));
    }

    #[test]
    fn heartbeat_lease_is_issued_after_persisted_epoch_map_tuple() {
        let tmp = test_util::tempdir();
        let store_path = tmp.path().join("control-plane.state");
        let store = FileControlPlaneStore::new(&store_path);
        let mut authority = SingleAuthorityControlPlane::open(store.clone()).unwrap();
        authority
            .set_node_membership(NodeId::new(3), NodeMembershipState::Active)
            .unwrap();
        let membership_epoch = authority.snapshot().cluster_epoch();
        let lease = authority
            .heartbeat(heartbeat(3, membership_epoch, 2_000), 2_000)
            .unwrap();

        let persisted = store.load().unwrap().unwrap();
        assert_eq!(
            persisted.authority_incarnation(),
            lease.authority_incarnation()
        );
        assert_eq!(persisted.cluster_epoch(), lease.cluster_epoch());
        let persisted_node = persisted.node(NodeId::new(3)).unwrap();
        assert_eq!(
            persisted_node.lease_deadline_ms(),
            Some(lease.lease_deadline_ms())
        );
        assert_eq!(
            persisted_node.availability(),
            NodeAvailabilityState::Healthy
        );
        assert_eq!(persisted_node.last_observed_epoch(), Some(membership_epoch));
        assert!(!lease.serving());
        assert_eq!(
            lease.snapshot().cluster_epoch(),
            authority.snapshot().cluster_epoch()
        );
        assert!(std::fs::metadata(store_path).unwrap().is_file());
    }

    #[test]
    fn heartbeat_rejects_stale_node_incarnation_and_fences_new_incarnation() {
        let tmp = test_util::tempdir();
        let store = FileControlPlaneStore::new(tmp.path().join("control-plane.state"));
        let mut authority = SingleAuthorityControlPlane::open(store).unwrap();
        authority
            .set_node_membership(NodeId::new(4), NodeMembershipState::Active)
            .unwrap();
        let first = authority
            .heartbeat(heartbeat(4, authority.snapshot().cluster_epoch(), 100), 100)
            .unwrap();

        let mut stale = heartbeat(4, first.cluster_epoch(), 200);
        stale.node_incarnation -= 1;
        assert!(matches!(
            authority.heartbeat(stale, 200),
            Err(ControlPlaneError::StaleNodeIncarnation { node_id: 4, .. })
        ));

        let mut restarted_node = heartbeat(4, first.cluster_epoch(), 300);
        restarted_node.node_incarnation += 1;
        let fenced = authority.heartbeat(restarted_node, 300).unwrap();
        assert!(fenced.cluster_epoch() > first.cluster_epoch());
        assert!(!fenced.serving());
        assert_eq!(
            authority
                .snapshot()
                .node(NodeId::new(4))
                .unwrap()
                .node_incarnation(),
            15
        );

        let caught_up = authority
            .heartbeat(
                heartbeat_from_record(&authority, 4, fenced.cluster_epoch(), 400),
                400,
            )
            .unwrap();
        assert!(caught_up.serving());
    }

    #[test]
    fn endpoint_change_bumps_epoch_and_requires_node_to_observe_new_map() {
        let tmp = test_util::tempdir();
        let store = FileControlPlaneStore::new(tmp.path().join("control-plane.state"));
        let mut authority = SingleAuthorityControlPlane::open(store).unwrap();
        authority
            .set_node_membership(NodeId::new(5), NodeMembershipState::Active)
            .unwrap();
        let serving = heartbeat_until_serving(&mut authority, 5, 100);
        assert!(serving.serving());

        let mut moved = heartbeat(5, serving.cluster_epoch(), 200);
        moved.endpoint = "node-5-new.sock".to_owned();
        let changed = authority.heartbeat(moved, 200).unwrap();
        assert!(changed.cluster_epoch() > serving.cluster_epoch());
        assert!(!changed.serving());
        assert_eq!(
            authority
                .snapshot()
                .node(NodeId::new(5))
                .unwrap()
                .endpoint(),
            "node-5-new.sock"
        );
        assert_eq!(
            authority.deterministic_pg_primary(PgId::new(1), &[NodeId::new(5)], 200),
            None
        );

        let caught_up = authority
            .heartbeat(
                heartbeat_from_record(&authority, 5, changed.cluster_epoch(), 300),
                300,
            )
            .unwrap();
        assert!(caught_up.serving());
        assert_eq!(
            authority.deterministic_pg_primary(PgId::new(1), &[NodeId::new(5)], 300),
            Some(NodeId::new(5))
        );
    }

    #[test]
    fn pg_acting_set_changes_start_in_peering_and_validate_nodes() {
        let tmp = test_util::tempdir();
        let store_path = tmp.path().join("control-plane.state");
        let store = FileControlPlaneStore::new(&store_path);
        let mut authority = SingleAuthorityControlPlane::open(store.clone()).unwrap();
        authority
            .set_node_membership(NodeId::new(1), NodeMembershipState::Active)
            .unwrap();
        authority
            .set_node_membership(NodeId::new(2), NodeMembershipState::Active)
            .unwrap();

        assert!(matches!(
            authority.set_pg_acting_set(PgId::new(7), Vec::new()),
            Err(ControlPlaneError::EmptyActingSet { pg_id: 7 })
        ));
        assert!(matches!(
            authority.set_pg_acting_set(PgId::new(7), vec![NodeId::new(99)]),
            Err(ControlPlaneError::UnknownActingSetNode {
                pg_id: 7,
                node_id: 99
            })
        ));
        assert!(matches!(
            authority.set_pg_acting_set(PgId::new(7), vec![NodeId::new(1), NodeId::new(1)]),
            Err(ControlPlaneError::DuplicateActingSetNode {
                pg_id: 7,
                node_id: 1
            })
        ));

        let before = authority.snapshot().cluster_epoch();
        authority
            .set_pg_acting_set(PgId::new(7), vec![NodeId::new(1), NodeId::new(2)])
            .unwrap();
        assert!(authority.snapshot().cluster_epoch() > before);
        let pg = authority.snapshot().pg(PgId::new(7)).unwrap();
        assert_eq!(pg.pg_id(), PgId::new(7));
        assert_eq!(pg.state(), PgState::Peering);
        assert_eq!(pg.acting_set(), &[NodeId::new(1), NodeId::new(2)]);
        assert_eq!(authority.serving_pg_primary(PgId::new(7), 1), None);

        let persisted = store.load().unwrap().unwrap();
        let persisted_pg = persisted.pg(PgId::new(7)).unwrap();
        assert_eq!(persisted_pg.state(), PgState::Peering);
        assert_eq!(persisted_pg.acting_set(), &[NodeId::new(1), NodeId::new(2)]);
        assert!(std::fs::metadata(store_path).unwrap().is_file());
    }

    #[test]
    fn bootstrap_initial_cluster_map_persists_nodes_and_pg_routes_atomically() {
        let tmp = test_util::tempdir();
        let store = FileControlPlaneStore::new(tmp.path().join("control-plane.state"));
        let mut authority = SingleAuthorityControlPlane::open(store).unwrap();

        let snapshot = authority
            .bootstrap_initial_cluster_map(
                vec![
                    (NodeId::new(2), "/tmp/node-2.sock".to_owned()),
                    (NodeId::new(4), "/tmp/node-4.sock".to_owned()),
                ],
                vec![PgId::new(0), PgId::new(3)],
            )
            .unwrap();

        assert_eq!(
            snapshot
                .nodes()
                .map(NodeControlRecord::node_id)
                .collect::<Vec<_>>(),
            vec![NodeId::new(2), NodeId::new(4)]
        );
        assert_eq!(
            snapshot
                .nodes()
                .map(NodeControlRecord::endpoint)
                .collect::<Vec<_>>(),
            vec!["/tmp/node-2.sock", "/tmp/node-4.sock"]
        );
        for pg_id in [0, 3] {
            let pg = snapshot.pg(PgId::new(pg_id)).unwrap();
            assert_eq!(pg.acting_set(), &[NodeId::new(2), NodeId::new(4)]);
            assert_eq!(pg.state(), PgState::Peering);
        }
        assert_eq!(
            snapshot
                .runtime_map(1_000)
                .unwrap()
                .nodes()
                .iter()
                .map(NodeRouteSnapshot::endpoint)
                .collect::<Vec<_>>(),
            vec!["/tmp/node-2.sock", "/tmp/node-4.sock"]
        );
        assert!(matches!(
            authority.bootstrap_initial_cluster_map(
                vec![(NodeId::new(5), "/tmp/node-5.sock".to_owned())],
                vec![PgId::new(7)]
            ),
            Err(ControlPlaneError::BootstrapRequiresEmptyState)
        ));
    }

    #[test]
    fn active_pg_primary_comes_from_authoritative_acting_set() {
        let tmp = test_util::tempdir();
        let store = FileControlPlaneStore::new(tmp.path().join("control-plane.state"));
        let mut authority = SingleAuthorityControlPlane::open(store).unwrap();
        for node_id in [1, 2] {
            authority
                .set_node_membership(NodeId::new(node_id), NodeMembershipState::Active)
                .unwrap();
            assert!(heartbeat_until_serving(&mut authority, node_id, 1_000).serving());
        }
        authority
            .heartbeat(
                heartbeat_from_record(&authority, 1, authority.snapshot().cluster_epoch(), 1_002),
                1_002,
            )
            .unwrap();
        authority
            .set_pg_acting_set(PgId::new(8), vec![NodeId::new(1), NodeId::new(2)])
            .unwrap();
        assert_eq!(authority.serving_pg_primary(PgId::new(8), 1_002), None);

        assert!(matches!(
            authority.set_pg_state(PgId::new(8), PgState::Active),
            Err(ControlPlaneError::ActivePgRequiresPeeringComplete { pg_id: 8 })
        ));
        assert_eq!(authority.serving_pg_primary(PgId::new(8), 1_002), None);
        for node_id in [1, 2] {
            heartbeat_with_pg_observation(
                &mut authority,
                node_id,
                8,
                PgState::Peering,
                2_000 + u64::from(node_id),
            );
        }
        assert!(matches!(
            authority.complete_pg_peering(
                PgId::new(8),
                NodeId::new(2),
                node_incarnation(&authority, 2),
                2_050,
            ),
            Err(ControlPlaneError::PgPrimaryNotServingCurrentEpoch {
                pg_id: 8,
                node_id: 2
            })
        ));
        assert!(matches!(
            authority.complete_pg_peering(PgId::new(8), NodeId::new(99), 99, 2_050),
            Err(ControlPlaneError::PgPrimaryNotInActingSet {
                pg_id: 8,
                node_id: 99
            })
        ));
        authority
            .complete_pg_peering(
                PgId::new(8),
                NodeId::new(1),
                node_incarnation(&authority, 1),
                2_050,
            )
            .unwrap();
        assert_eq!(authority.serving_pg_primary(PgId::new(8), 2_050), None);
        heartbeat_with_pg_observation(&mut authority, 1, 8, PgState::Active, 3_001);
        authority
            .heartbeat(
                heartbeat_from_record(&authority, 2, authority.snapshot().cluster_epoch(), 3_002),
                3_002,
            )
            .unwrap();
        assert_eq!(
            authority.serving_pg_primary(PgId::new(8), 3_002),
            Some(NodeId::new(1))
        );
    }

    #[test]
    fn heartbeat_refresh_completes_ready_peering_for_storage_node_before_frontend_export() {
        let tmp = test_util::tempdir();
        let store = FileControlPlaneStore::new(tmp.path().join("control-plane.state"));
        let mut authority = SingleAuthorityControlPlane::open(store).unwrap();
        authority
            .set_node_membership(NodeId::new(1), NodeMembershipState::Active)
            .unwrap();
        assert!(heartbeat_until_serving(&mut authority, 1, 1_000).serving());
        authority
            .set_pg_acting_set(PgId::new(22), vec![NodeId::new(1)])
            .unwrap();

        let mut peering_heartbeat =
            heartbeat_from_record(&authority, 1, authority.snapshot().cluster_epoch(), 2_000);
        peering_heartbeat.pg_observations = vec![NodePgHeartbeatObservation {
            pg_id: PgId::new(22),
            state: PgState::Peering,
            metadata_proof: PgMetadataProof::empty(),
        }];
        let refresh = authority
            .refresh_node_heartbeat(peering_heartbeat, 2_000)
            .unwrap();

        let pg = authority.snapshot().pg(PgId::new(22)).unwrap();
        assert_eq!(pg.state(), PgState::Active);
        assert_eq!(pg.active_primary(), Some(NodeId::new(1)));
        assert_eq!(
            refresh.lease().cluster_epoch(),
            authority.snapshot().cluster_epoch()
        );
        assert!(
            !refresh.lease().serving(),
            "peering completion bumps the epoch before the node observes it"
        );
        let storage_route = refresh
            .runtime_map()
            .pg_routes()
            .iter()
            .find(|route| route.pg_id() == PgId::new(22))
            .unwrap();
        assert_eq!(storage_route.state(), PgState::Active);
        assert!(matches!(
            authority.snapshot().runtime_map(2_001),
            Err(ControlPlaneError::PgHasNoServingPrimary { pg_id: 22, .. })
        ));

        let mut active_heartbeat =
            heartbeat_from_record(&authority, 1, authority.snapshot().cluster_epoch(), 2_002);
        active_heartbeat.pg_observations = vec![NodePgHeartbeatObservation {
            pg_id: PgId::new(22),
            state: PgState::Active,
            metadata_proof: PgMetadataProof::empty(),
        }];
        let refresh = authority
            .refresh_node_heartbeat(active_heartbeat, 2_002)
            .unwrap();
        assert!(refresh.lease().serving());
        let frontend_map = authority.snapshot().runtime_map(2_003).unwrap();
        assert_eq!(
            frontend_map
                .pg_routes()
                .iter()
                .find(|route| route.pg_id() == PgId::new(22))
                .unwrap()
                .state(),
            PgState::Active
        );
    }

    #[test]
    fn active_pg_primary_is_bound_by_peering_completion() {
        let tmp = test_util::tempdir();
        let store = FileControlPlaneStore::new(tmp.path().join("control-plane.state"));
        let mut authority = SingleAuthorityControlPlane::open(store).unwrap();
        for node_id in [1, 2] {
            authority
                .set_node_membership(NodeId::new(node_id), NodeMembershipState::Active)
                .unwrap();
            assert!(heartbeat_until_serving(&mut authority, node_id, 1_000).serving());
        }
        authority
            .set_pg_acting_set(PgId::new(20), vec![NodeId::new(1), NodeId::new(2)])
            .unwrap();
        for node_id in [1, 2] {
            heartbeat_with_pg_observation(
                &mut authority,
                node_id,
                20,
                PgState::Peering,
                2_000 + u64::from(node_id),
            );
        }
        authority
            .complete_pg_peering(
                PgId::new(20),
                NodeId::new(1),
                node_incarnation(&authority, 1),
                2_050,
            )
            .unwrap();
        assert_eq!(
            authority
                .snapshot()
                .pg(PgId::new(20))
                .unwrap()
                .active_primary(),
            Some(NodeId::new(1))
        );

        heartbeat_with_pg_observation(&mut authority, 2, 20, PgState::Active, 3_001);
        assert_eq!(authority.serving_pg_primary(PgId::new(20), 3_001), None);
        assert!(matches!(
            authority.complete_pg_peering(
                PgId::new(20),
                NodeId::new(2),
                node_incarnation(&authority, 2),
                3_002,
            ),
            Err(ControlPlaneError::PgNotPeering {
                pg_id: 20,
                state: PgState::Active,
                ..
            })
        ));

        heartbeat_with_pg_observation(&mut authority, 1, 20, PgState::Active, 3_003);
        assert_eq!(
            authority.serving_pg_primary(PgId::new(20), 3_003),
            Some(NodeId::new(1))
        );
        authority
            .complete_pg_peering(
                PgId::new(20),
                NodeId::new(1),
                node_incarnation(&authority, 1),
                3_004,
            )
            .unwrap();
    }

    #[test]
    fn active_pg_route_requires_bound_primary_and_active_observation() {
        let tmp = test_util::tempdir();
        let store = FileControlPlaneStore::new(tmp.path().join("control-plane.state"));
        let mut authority = SingleAuthorityControlPlane::open(store).unwrap();
        authority
            .set_node_membership(NodeId::new(1), NodeMembershipState::Active)
            .unwrap();
        assert!(heartbeat_until_serving(&mut authority, 1, 1_000).serving());
        authority
            .set_pg_acting_set(PgId::new(21), vec![NodeId::new(1)])
            .unwrap();
        heartbeat_with_pg_observation(&mut authority, 1, 21, PgState::Peering, 2_000);
        authority
            .complete_pg_peering(
                PgId::new(21),
                NodeId::new(1),
                node_incarnation(&authority, 1),
                2_001,
            )
            .unwrap();

        assert!(matches!(
            authority.snapshot().active_pg_route(PgId::new(21), 2_002),
            Err(ControlPlaneError::PgHasNoServingPrimary { pg_id: 21, .. })
        ));

        heartbeat_with_pg_observation(&mut authority, 1, 21, PgState::Active, 2_003);
        let route = authority
            .snapshot()
            .active_pg_route(PgId::new(21), 2_004)
            .unwrap();
        assert_eq!(route.cluster_epoch(), authority.snapshot().cluster_epoch());
        assert_eq!(route.pg_id(), PgId::new(21));
        assert_eq!(route.primary_node_id(), NodeId::new(1));
        assert_eq!(route.acting_set(), &[NodeId::new(1)]);
        assert_eq!(route.state(), PgState::Active);
        assert_eq!(
            route.primary_lease_deadline_ms(),
            authority
                .snapshot()
                .node(NodeId::new(1))
                .unwrap()
                .lease_deadline_ms()
        );
        assert_eq!(
            authority.snapshot().active_pg_routes(2_004).unwrap(),
            vec![route.clone()]
        );

        let local_route = crate::cluster::LocalPgRoute::from(&route);
        assert_eq!(local_route.cluster_epoch(), route.cluster_epoch());
        assert_eq!(local_route.pg_id(), route.pg_id());
        assert_eq!(local_route.primary_node_id(), route.primary_node_id());
        assert_eq!(local_route.acting_set(), route.acting_set());
        assert_eq!(local_route.state(), route.state());

        let storage_node_route = crate::storage_node_server::StorageNodePgRoute::from(&route);
        assert_eq!(storage_node_route.cluster_epoch, route.cluster_epoch());
        assert_eq!(storage_node_route.pg_id, route.pg_id().get());
        assert_eq!(storage_node_route.primary_node_id, route.primary_node_id());
        assert_eq!(storage_node_route.acting_set, route.acting_set());
        assert_eq!(storage_node_route.state, route.state());
    }

    #[test]
    fn active_primary_service_requires_current_observation_metadata_proof() {
        let tmp = test_util::tempdir();
        let store = FileControlPlaneStore::new(tmp.path().join("control-plane.state"));
        let mut authority = SingleAuthorityControlPlane::open(store).unwrap();
        authority
            .set_node_membership(NodeId::new(1), NodeMembershipState::Active)
            .unwrap();
        assert!(heartbeat_until_serving(&mut authority, 1, 1_000).serving());
        authority
            .set_pg_acting_set(PgId::new(22), vec![NodeId::new(1)])
            .unwrap();

        let accepted_proof = PgMetadataProof {
            applied_log_index: 42,
            applied_log_hash: 0xabc,
            state_digest: 0xdef,
        };
        let mut peering_heartbeat =
            heartbeat_from_record(&authority, 1, authority.snapshot().cluster_epoch(), 2_000);
        peering_heartbeat.pg_observations = vec![NodePgHeartbeatObservation {
            pg_id: PgId::new(22),
            state: PgState::Peering,
            metadata_proof: accepted_proof,
        }];
        authority.heartbeat(peering_heartbeat, 2_000).unwrap();
        authority
            .complete_pg_peering(
                PgId::new(22),
                NodeId::new(1),
                node_incarnation(&authority, 1),
                2_010,
            )
            .unwrap();

        let mut active_heartbeat =
            heartbeat_from_record(&authority, 1, authority.snapshot().cluster_epoch(), 2_020);
        active_heartbeat.pg_observations = vec![NodePgHeartbeatObservation {
            pg_id: PgId::new(22),
            state: PgState::Active,
            metadata_proof: accepted_proof,
        }];
        authority.heartbeat(active_heartbeat, 2_020).unwrap();
        assert!(authority
            .snapshot()
            .active_pg_route(PgId::new(22), 2_030)
            .is_ok());
        assert_eq!(
            authority.serving_pg_primary(PgId::new(22), 2_030),
            Some(NodeId::new(1))
        );
        assert!(authority
            .authorize_pg_operation(
                PgServiceOperation::MetadataWrite,
                PgId::new(22),
                NodeId::new(1),
                node_incarnation(&authority, 1),
                authority.snapshot().cluster_epoch(),
                2_030,
            )
            .is_ok());

        let mismatched_proof = PgMetadataProof {
            applied_log_index: 43,
            applied_log_hash: 0xabc,
            state_digest: 0xdef,
        };
        authority
            .snapshot
            .nodes
            .get_mut(&NodeId::new(1))
            .unwrap()
            .pg_observations
            .get_mut(&PgId::new(22))
            .unwrap()
            .metadata_proof = mismatched_proof;

        assert_eq!(authority.serving_pg_primary(PgId::new(22), 2_031), None);
        assert!(matches!(
            authority.snapshot().active_pg_route(PgId::new(22), 2_031),
            Err(ControlPlaneError::PgActiveMetadataProofMismatch {
                pg_id: 22,
                node_id: 1,
                expected,
                actual,
                ..
            }) if expected == accepted_proof && actual == mismatched_proof
        ));
        assert!(matches!(
            authority
                .snapshot()
                .runtime_map_for_storage_node_refresh(2_031, &BTreeSet::new()),
            Err(ControlPlaneError::PgActiveMetadataProofMismatch {
                pg_id: 22,
                node_id: 1,
                expected,
                actual,
                ..
            }) if expected == accepted_proof && actual == mismatched_proof
        ));
        assert!(matches!(
            authority.authorize_pg_operation(
                PgServiceOperation::MetadataWrite,
                PgId::new(22),
                NodeId::new(1),
                node_incarnation(&authority, 1),
                authority.snapshot().cluster_epoch(),
                2_031,
            ),
            Err(ControlPlaneError::PgActiveMetadataProofMismatch {
                pg_id: 22,
                node_id: 1,
                expected,
                actual,
                ..
            }) if expected == accepted_proof && actual == mismatched_proof
        ));
    }

    #[test]
    fn active_pg_route_fails_closed_for_peering_pg() {
        let tmp = test_util::tempdir();
        let store = FileControlPlaneStore::new(tmp.path().join("control-plane.state"));
        let mut authority = SingleAuthorityControlPlane::open(store).unwrap();
        authority
            .set_node_membership(NodeId::new(1), NodeMembershipState::Active)
            .unwrap();
        assert!(heartbeat_until_serving(&mut authority, 1, 1_000).serving());
        authority
            .set_pg_acting_set(PgId::new(22), vec![NodeId::new(1)])
            .unwrap();

        assert!(matches!(
            authority.snapshot().active_pg_route(PgId::new(22), 1_001),
            Err(ControlPlaneError::PgNotActive {
                pg_id: 22,
                state: PgState::Peering,
                ..
            })
        ));
        assert!(authority
            .snapshot()
            .active_pg_routes(1_001)
            .unwrap()
            .is_empty());
    }

    #[test]
    fn pg_route_exports_peering_pg_for_fail_closed_runtime_install() {
        let tmp = test_util::tempdir();
        let store = FileControlPlaneStore::new(tmp.path().join("control-plane.state"));
        let mut authority = SingleAuthorityControlPlane::open(store).unwrap();
        for node_id in [1, 2] {
            authority
                .set_node_membership(NodeId::new(node_id), NodeMembershipState::Active)
                .unwrap();
            assert!(heartbeat_until_serving(&mut authority, node_id, 1_000).serving());
        }
        authority
            .set_pg_acting_set(PgId::new(24), vec![NodeId::new(2), NodeId::new(1)])
            .unwrap();

        let route = authority.snapshot().pg_route(PgId::new(24), 1_001).unwrap();
        assert_eq!(route.cluster_epoch(), authority.snapshot().cluster_epoch());
        assert_eq!(route.pg_id(), PgId::new(24));
        assert_eq!(route.primary_node_id(), NodeId::new(2));
        assert_eq!(route.acting_set(), &[NodeId::new(2), NodeId::new(1)]);
        assert_eq!(route.state(), PgState::Peering);
        assert_eq!(route.primary_lease_deadline_ms(), None);
        assert_eq!(
            authority.snapshot().pg_routes(1_001).unwrap(),
            vec![route.clone()]
        );

        let local_route = crate::cluster::LocalPgRoute::from(&route);
        let local_map =
            crate::cluster::LocalClusterMap::open_frontend_topology_only_with_pg_routes(
                NodeId::new(1),
                [NodeId::new(1), NodeId::new(2)],
                &[24],
                crate::EcShape { k: 1, m: 1 },
                authority.snapshot().cluster_epoch(),
                vec![local_route],
            )
            .unwrap();
        assert!(matches!(
            local_map.metadata_pg_primary_node(authority.snapshot().cluster_epoch(), PgId::new(24)),
            Err(crate::StoreError::PgNotActive {
                pg_id: 24,
                state: PgState::Peering,
                ..
            })
        ));
    }

    #[test]
    fn runtime_map_exports_pg_routes_with_routed_node_endpoints() {
        let tmp = test_util::tempdir();
        let store = FileControlPlaneStore::new(tmp.path().join("control-plane.state"));
        let mut authority = SingleAuthorityControlPlane::open(store).unwrap();
        for node_id in [1, 2] {
            authority
                .set_node_membership(NodeId::new(node_id), NodeMembershipState::Active)
                .unwrap();
            assert!(heartbeat_until_serving(&mut authority, node_id, 1_000).serving());
        }
        authority
            .set_pg_acting_set(PgId::new(25), vec![NodeId::new(2), NodeId::new(1)])
            .unwrap();

        let runtime_map = authority.snapshot().runtime_map(1_001).unwrap();
        assert_eq!(
            runtime_map.cluster_epoch(),
            authority.snapshot().cluster_epoch()
        );
        assert_eq!(runtime_map.valid_until_ms(), None);
        assert_eq!(runtime_map.pg_routes().len(), 1);
        let route = &runtime_map.pg_routes()[0];
        assert_eq!(route.pg_id(), PgId::new(25));
        assert_eq!(route.state(), PgState::Peering);
        assert_eq!(route.primary_node_id(), NodeId::new(2));
        assert_eq!(route.acting_set(), &[NodeId::new(2), NodeId::new(1)]);
        assert_eq!(
            runtime_map
                .nodes()
                .iter()
                .map(NodeRouteSnapshot::node_id)
                .collect::<Vec<_>>(),
            vec![NodeId::new(1), NodeId::new(2)]
        );
        assert_eq!(runtime_map.nodes()[0].endpoint(), "node-1.sock");
        assert_eq!(runtime_map.nodes()[1].endpoint(), "node-2.sock");
        assert_eq!(
            runtime_map.nodes()[0].node_incarnation(),
            node_incarnation(&authority, 1)
        );
    }

    #[test]
    fn runtime_map_valid_until_is_minimum_active_primary_lease_deadline() {
        let tmp = test_util::tempdir();
        let store = FileControlPlaneStore::new(tmp.path().join("control-plane.state"));
        let mut authority = SingleAuthorityControlPlane::open(store).unwrap();
        for node_id in [1, 2] {
            authority
                .set_node_membership(NodeId::new(node_id), NodeMembershipState::Active)
                .unwrap();
            assert!(heartbeat_until_serving(&mut authority, node_id, 1_000).serving());
        }
        authority
            .set_pg_acting_set(PgId::new(27), vec![NodeId::new(1)])
            .unwrap();
        authority
            .set_pg_acting_set(PgId::new(28), vec![NodeId::new(2)])
            .unwrap();
        heartbeat_with_pg_observation(&mut authority, 1, 27, PgState::Peering, 2_000);
        authority
            .complete_pg_peering(
                PgId::new(27),
                NodeId::new(1),
                node_incarnation(&authority, 1),
                2_001,
            )
            .unwrap();
        heartbeat_with_pg_observation(&mut authority, 1, 27, PgState::Active, 2_002);
        heartbeat_with_pg_observation(&mut authority, 2, 28, PgState::Peering, 3_000);
        authority
            .complete_pg_peering(
                PgId::new(28),
                NodeId::new(2),
                node_incarnation(&authority, 2),
                3_001,
            )
            .unwrap();
        heartbeat_with_pg_observation(&mut authority, 2, 28, PgState::Active, 3_002);
        heartbeat_with_pg_observation(&mut authority, 1, 27, PgState::Active, 3_003);

        let node_1_deadline = authority
            .snapshot()
            .node(NodeId::new(1))
            .unwrap()
            .lease_deadline_ms()
            .unwrap();
        let node_2_deadline = authority
            .snapshot()
            .node(NodeId::new(2))
            .unwrap()
            .lease_deadline_ms()
            .unwrap();
        assert!(node_2_deadline < node_1_deadline);

        let runtime_map = authority.snapshot().runtime_map(3_004).unwrap();
        assert_eq!(runtime_map.valid_until_ms(), Some(node_2_deadline));
        assert_eq!(
            runtime_map
                .pg_routes()
                .iter()
                .filter(|route| route.state() == PgState::Active)
                .map(PgRouteSnapshot::primary_lease_deadline_ms)
                .collect::<Vec<_>>(),
            vec![Some(node_1_deadline), Some(node_2_deadline)]
        );
    }

    #[test]
    fn runtime_map_builds_frontend_topology_with_validity_bound() {
        let tmp = test_util::tempdir();
        let store = FileControlPlaneStore::new(tmp.path().join("control-plane.state"));
        let mut authority = SingleAuthorityControlPlane::open(store).unwrap();
        for node_id in [1, 2] {
            authority
                .set_node_membership(NodeId::new(node_id), NodeMembershipState::Active)
                .unwrap();
            assert!(heartbeat_until_serving(&mut authority, node_id, 1_000).serving());
        }
        authority
            .set_pg_acting_set(PgId::new(29), vec![NodeId::new(2), NodeId::new(1)])
            .unwrap();
        heartbeat_with_pg_observation(&mut authority, 2, 29, PgState::Peering, 2_000);
        authority
            .complete_pg_peering(
                PgId::new(29),
                NodeId::new(2),
                node_incarnation(&authority, 2),
                2_001,
            )
            .unwrap();
        heartbeat_with_pg_observation(&mut authority, 2, 29, PgState::Active, 2_002);
        let runtime_map = authority.snapshot().runtime_map(2_003).unwrap();
        let valid_until_ms = runtime_map
            .valid_until_ms()
            .expect("active runtime map should have a validity deadline");

        let local_map =
            crate::cluster::LocalClusterMap::open_frontend_topology_only_with_runtime_map(
                NodeId::new(1),
                &runtime_map,
                crate::EcShape { k: 1, m: 1 },
            )
            .unwrap();

        assert_eq!(local_map.epoch(), runtime_map.cluster_epoch());
        assert_eq!(local_map.route_map_valid_until_ms(), Some(valid_until_ms));
        assert!(local_map.is_route_map_valid_at(valid_until_ms - 1));
        assert!(!local_map.is_route_map_valid_at(valid_until_ms));
        let route = local_map.pg_route(PgId::new(29)).unwrap();
        assert_eq!(route.primary_node_id(), NodeId::new(2));
        assert_eq!(route.acting_set(), &[NodeId::new(2), NodeId::new(1)]);
        assert_eq!(route.state(), PgState::Active);
    }

    #[test]
    fn runtime_map_builds_storage_cluster_with_validity_bound() {
        let tmp = test_util::tempdir();
        let store = FileControlPlaneStore::new(tmp.path().join("control-plane.state"));
        let mut authority = SingleAuthorityControlPlane::open(store).unwrap();
        authority
            .set_node_membership(NodeId::new(1), NodeMembershipState::Active)
            .unwrap();
        let endpoint = tmp
            .path()
            .join("node-1.sock")
            .to_string_lossy()
            .into_owned();
        assert!(
            heartbeat_until_serving_with_endpoint(&mut authority, 1, 1_000, endpoint).serving()
        );
        authority
            .set_pg_acting_set(PgId::new(31), vec![NodeId::new(1)])
            .unwrap();
        heartbeat_with_pg_observation(&mut authority, 1, 31, PgState::Peering, 2_000);
        authority
            .complete_pg_peering(
                PgId::new(31),
                NodeId::new(1),
                node_incarnation(&authority, 1),
                2_001,
            )
            .unwrap();
        heartbeat_with_pg_observation(&mut authority, 1, 31, PgState::Active, 2_002);
        let runtime_map = authority.snapshot().runtime_map(2_003).unwrap();
        let valid_until_ms = runtime_map.valid_until_ms().unwrap();

        let cluster = crate::StorageCluster::from_runtime_map(
            NodeId::new(1),
            &runtime_map,
            crate::EcShape { k: 1, m: 0 },
        )
        .unwrap();

        assert_eq!(cluster.cluster_epoch(), runtime_map.cluster_epoch());
        assert_eq!(cluster.operation_epoch(), runtime_map.cluster_epoch());
        assert_eq!(cluster.route_map_valid_until_ms(), Some(valid_until_ms));
        cluster
            .require_route_map_valid_at(valid_until_ms - 1)
            .unwrap();
        assert!(matches!(
            cluster.require_route_map_valid_at(valid_until_ms),
            Err(crate::StoreError::RouteMapExpired {
                cluster_epoch,
                valid_until_ms: expired_at,
                now_ms,
            }) if cluster_epoch == runtime_map.cluster_epoch()
                && expired_at == valid_until_ms
                && now_ms == valid_until_ms
        ));
        let route = cluster.local_pg_route(PgId::new(31)).unwrap();
        assert_eq!(route.primary_node_id(), NodeId::new(1));
        assert_eq!(route.state(), PgState::Active);
    }

    #[test]
    fn storage_cluster_refreshes_from_control_plane_runtime_map() {
        let tmp = test_util::tempdir();
        let store = FileControlPlaneStore::new(tmp.path().join("control-plane.state"));
        let mut authority = SingleAuthorityControlPlane::open(store).unwrap();
        authority
            .set_node_membership(NodeId::new(1), NodeMembershipState::Active)
            .unwrap();
        assert!(heartbeat_until_serving(&mut authority, 1, 1_000).serving());
        authority
            .set_pg_acting_set(PgId::new(31), vec![NodeId::new(1)])
            .unwrap();
        heartbeat_with_pg_observation(&mut authority, 1, 31, PgState::Peering, 2_000);

        let peering_map = authority.snapshot().runtime_map(2_001).unwrap();
        let cluster = crate::StorageCluster::from_runtime_map(
            NodeId::new(1),
            &peering_map,
            crate::EcShape { k: 1, m: 0 },
        )
        .unwrap();
        assert_eq!(
            cluster.local_pg_route(PgId::new(31)).unwrap().state(),
            PgState::Peering
        );
        assert_eq!(cluster.route_map_valid_until_ms(), None);

        authority
            .complete_pg_peering(
                PgId::new(31),
                NodeId::new(1),
                node_incarnation(&authority, 1),
                2_002,
            )
            .unwrap();
        heartbeat_with_pg_observation(&mut authority, 1, 31, PgState::Active, 2_003);

        let refreshed = cluster
            .refresh_from_control_plane_runtime_map(&authority, 2_004)
            .unwrap();
        assert_eq!(
            refreshed.cluster_epoch(),
            authority.snapshot().cluster_epoch()
        );
        assert_eq!(
            refreshed.local_pg_route(PgId::new(31)).unwrap().state(),
            PgState::Active
        );
        assert_eq!(
            refreshed.route_map_valid_until_ms(),
            authority
                .snapshot()
                .runtime_map(2_004)
                .unwrap()
                .valid_until_ms()
        );
        assert!(refreshed.route_map_valid_until_ms().is_some());
    }

    #[test]
    fn storage_cluster_runtime_map_handle_refresh_installs_current_map() {
        let tmp = test_util::tempdir();
        let store = FileControlPlaneStore::new(tmp.path().join("control-plane.state"));
        let mut authority = SingleAuthorityControlPlane::open(store).unwrap();
        authority
            .set_node_membership(NodeId::new(1), NodeMembershipState::Active)
            .unwrap();
        assert!(heartbeat_until_serving(&mut authority, 1, 1_000).serving());
        authority
            .set_pg_acting_set(PgId::new(31), vec![NodeId::new(1)])
            .unwrap();
        heartbeat_with_pg_observation(&mut authority, 1, 31, PgState::Peering, 2_000);

        let peering_map = authority.snapshot().runtime_map(2_001).unwrap();
        let cluster = crate::StorageCluster::from_runtime_map(
            NodeId::new(1),
            &peering_map,
            crate::EcShape { k: 1, m: 0 },
        )
        .unwrap();
        let handle = crate::StorageClusterRuntimeMapHandle::new(cluster);
        assert_eq!(
            handle
                .current()
                .local_pg_route(PgId::new(31))
                .unwrap()
                .state(),
            PgState::Peering
        );

        authority
            .complete_pg_peering(
                PgId::new(31),
                NodeId::new(1),
                node_incarnation(&authority, 1),
                2_002,
            )
            .unwrap();
        heartbeat_with_pg_observation(&mut authority, 1, 31, PgState::Active, 2_003);

        let refreshed = handle
            .refresh_from_control_plane_runtime_map(&authority, 2_004)
            .unwrap();
        assert_eq!(Arc::as_ptr(&handle.current()), Arc::as_ptr(&refreshed));
        assert_eq!(
            handle
                .current()
                .local_pg_route(PgId::new(31))
                .unwrap()
                .state(),
            PgState::Active
        );
        assert!(handle.current().route_map_valid_until_ms().is_some());
    }

    #[test]
    fn storage_cluster_runtime_map_handle_rejects_epoch_downgrade() {
        let tmp = test_util::tempdir();
        let store = FileControlPlaneStore::new(tmp.path().join("control-plane.state"));
        let mut authority = SingleAuthorityControlPlane::open(store).unwrap();
        authority
            .set_node_membership(NodeId::new(1), NodeMembershipState::Active)
            .unwrap();
        assert!(heartbeat_until_serving(&mut authority, 1, 1_000).serving());
        authority
            .set_pg_acting_set(PgId::new(31), vec![NodeId::new(1)])
            .unwrap();
        heartbeat_with_pg_observation(&mut authority, 1, 31, PgState::Peering, 2_000);

        let older_map = authority.snapshot().runtime_map(2_001).unwrap();
        let older_cluster = crate::StorageCluster::from_runtime_map(
            NodeId::new(1),
            &older_map,
            crate::EcShape { k: 1, m: 0 },
        )
        .unwrap();
        let handle = crate::StorageClusterRuntimeMapHandle::new(Arc::clone(&older_cluster));

        authority
            .set_node_membership(NodeId::new(2), NodeMembershipState::Active)
            .unwrap();
        assert!(heartbeat_until_serving(&mut authority, 2, 3_000).serving());
        let newer_map = authority.snapshot().runtime_map(3_001).unwrap();
        let newer_cluster = crate::StorageCluster::from_runtime_map(
            NodeId::new(1),
            &newer_map,
            crate::EcShape { k: 1, m: 0 },
        )
        .unwrap();
        assert!(newer_cluster.cluster_epoch() > older_cluster.cluster_epoch());
        handle.install(Arc::clone(&newer_cluster)).unwrap();

        assert!(matches!(
            handle.install(older_cluster),
            Err(crate::cluster::StorageClusterRuntimeMapRefreshError::EpochDowngrade {
                current,
                candidate,
            }) if current == newer_cluster.cluster_epoch() && candidate == older_map.cluster_epoch()
        ));
        assert_eq!(
            handle.current().cluster_epoch(),
            newer_cluster.cluster_epoch()
        );
    }

    #[test]
    fn storage_cluster_runtime_map_handle_rejects_same_epoch_validity_regression() {
        let tmp = test_util::tempdir();
        let store = FileControlPlaneStore::new(tmp.path().join("control-plane.state"));
        let mut authority = SingleAuthorityControlPlane::open(store).unwrap();
        authority
            .set_node_membership(NodeId::new(1), NodeMembershipState::Active)
            .unwrap();
        assert!(heartbeat_until_serving(&mut authority, 1, 1_000).serving());
        authority
            .set_pg_acting_set(PgId::new(31), vec![NodeId::new(1)])
            .unwrap();
        heartbeat_with_pg_observation(&mut authority, 1, 31, PgState::Peering, 2_000);
        authority
            .complete_pg_peering(
                PgId::new(31),
                NodeId::new(1),
                node_incarnation(&authority, 1),
                2_001,
            )
            .unwrap();
        heartbeat_with_pg_observation(&mut authority, 1, 31, PgState::Active, 2_002);

        let current_map = authority.snapshot().runtime_map(2_003).unwrap();
        let current_cluster = crate::StorageCluster::from_runtime_map(
            NodeId::new(1),
            &current_map,
            crate::EcShape { k: 1, m: 0 },
        )
        .unwrap();
        let handle = crate::StorageClusterRuntimeMapHandle::new(Arc::clone(&current_cluster));
        let current_valid_until = current_cluster.route_map_valid_until_ms().unwrap();

        let mut shorter_lease = heartbeat_from_record(
            &authority,
            1,
            authority.snapshot().cluster_epoch(),
            current_valid_until - 20,
        );
        shorter_lease.requested_lease_duration_ms = 10;
        shorter_lease.pg_observations = vec![NodePgHeartbeatObservation {
            pg_id: PgId::new(31),
            state: PgState::Active,
            metadata_proof: PgMetadataProof::empty(),
        }];
        authority
            .heartbeat(shorter_lease, current_valid_until - 20)
            .unwrap();
        let stale_map = authority
            .snapshot()
            .runtime_map(current_valid_until - 19)
            .unwrap();
        let stale_cluster = crate::StorageCluster::from_runtime_map(
            NodeId::new(1),
            &stale_map,
            crate::EcShape { k: 1, m: 0 },
        )
        .unwrap();
        assert_eq!(
            stale_cluster.cluster_epoch(),
            current_cluster.cluster_epoch()
        );
        assert!(
            stale_cluster.route_map_valid_until_ms() < current_cluster.route_map_valid_until_ms()
        );

        assert!(matches!(
            handle.install(stale_cluster),
            Err(crate::cluster::StorageClusterRuntimeMapRefreshError::ValidityRegression {
                current,
                candidate,
            }) if current == current_cluster.route_map_valid_until_ms()
                && candidate == stale_map.valid_until_ms()
        ));
        assert_eq!(
            handle.current().route_map_valid_until_ms(),
            Some(current_valid_until)
        );
    }

    #[test]
    fn storage_cluster_runtime_map_handle_rejects_unbounded_to_bounded_same_epoch() {
        let tmp = test_util::tempdir();
        let store = FileControlPlaneStore::new(tmp.path().join("control-plane.state"));
        let mut authority = SingleAuthorityControlPlane::open(store).unwrap();
        authority
            .set_node_membership(NodeId::new(1), NodeMembershipState::Active)
            .unwrap();
        assert!(heartbeat_until_serving(&mut authority, 1, 1_000).serving());
        authority
            .set_pg_acting_set(PgId::new(31), vec![NodeId::new(1)])
            .unwrap();
        heartbeat_with_pg_observation(&mut authority, 1, 31, PgState::Peering, 2_000);

        authority
            .complete_pg_peering(
                PgId::new(31),
                NodeId::new(1),
                node_incarnation(&authority, 1),
                2_002,
            )
            .unwrap();
        heartbeat_with_pg_observation(&mut authority, 1, 31, PgState::Active, 2_003);
        let active_map = authority.snapshot().runtime_map(2_004).unwrap();
        let active_cluster = crate::StorageCluster::from_runtime_map(
            NodeId::new(1),
            &active_map,
            crate::EcShape { k: 1, m: 0 },
        )
        .unwrap();
        let unbounded_local_map =
            crate::cluster::LocalClusterMap::open_frontend_topology_only_with_pg_routes(
                NodeId::new(1),
                [NodeId::new(1)],
                &[31],
                crate::EcShape { k: 1, m: 0 },
                active_map.cluster_epoch(),
                active_map
                    .pg_routes()
                    .iter()
                    .map(crate::cluster::LocalPgRoute::from),
            )
            .unwrap();
        let unbounded_cluster = crate::StorageCluster::test_from_local_map_with_epoch(
            Arc::new(unbounded_local_map),
            active_map.cluster_epoch(),
        )
        .unwrap();
        let handle = crate::StorageClusterRuntimeMapHandle::new(Arc::clone(&unbounded_cluster));
        assert_eq!(
            active_cluster.cluster_epoch(),
            unbounded_cluster.cluster_epoch()
        );
        assert_eq!(unbounded_cluster.route_map_valid_until_ms(), None);
        assert!(active_cluster.route_map_valid_until_ms().is_some());

        assert!(matches!(
            handle.install(active_cluster),
            Err(
                crate::cluster::StorageClusterRuntimeMapRefreshError::ValidityRegression {
                    current: None,
                    candidate: Some(_),
                }
            )
        ));
        assert_eq!(handle.current().route_map_valid_until_ms(), None);
    }

    #[test]
    fn storage_cluster_runtime_map_refresh_loop_installs_current_map() {
        let tmp = test_util::tempdir();
        let store = FileControlPlaneStore::new(tmp.path().join("control-plane.state"));
        let mut authority = SingleAuthorityControlPlane::open(store).unwrap();
        authority
            .set_node_membership(NodeId::new(1), NodeMembershipState::Active)
            .unwrap();
        assert!(heartbeat_until_serving(&mut authority, 1, 1_000).serving());
        authority
            .set_pg_acting_set(PgId::new(31), vec![NodeId::new(1)])
            .unwrap();
        heartbeat_with_pg_observation(&mut authority, 1, 31, PgState::Peering, 2_000);

        let peering_map = authority.snapshot().runtime_map(2_001).unwrap();
        let cluster = crate::StorageCluster::from_runtime_map(
            NodeId::new(1),
            &peering_map,
            crate::EcShape { k: 1, m: 0 },
        )
        .unwrap();
        let handle = crate::StorageClusterRuntimeMapHandle::new(cluster);

        authority
            .complete_pg_peering(
                PgId::new(31),
                NodeId::new(1),
                node_incarnation(&authority, 1),
                2_002,
            )
            .unwrap();
        heartbeat_with_pg_observation(&mut authority, 1, 31, PgState::Active, 2_003);
        let expected_runtime_map = authority.snapshot().runtime_map(2_004).unwrap();
        let now = Arc::new(AtomicU64::new(2_004));
        let loop_now = Arc::clone(&now);
        let mut refresh_loop = handle
            .clone()
            .spawn_control_plane_refresh_loop(authority, Duration::from_millis(5), move || {
                loop_now.fetch_add(1, Ordering::SeqCst)
            })
            .unwrap();

        let deadline = Instant::now() + Duration::from_secs(1);
        loop {
            if refresh_loop.status().successes > 0 {
                break;
            }
            assert!(
                Instant::now() < deadline,
                "frontend runtime-map refresh loop did not install a map: {:?}",
                refresh_loop.status()
            );
            std::thread::sleep(Duration::from_millis(1));
        }

        let current = handle.current();
        assert_eq!(
            current.cluster_epoch(),
            expected_runtime_map.cluster_epoch()
        );
        assert_eq!(
            current.local_pg_route(PgId::new(31)).unwrap().state(),
            PgState::Active
        );
        assert_eq!(
            current.route_map_valid_until_ms(),
            expected_runtime_map.valid_until_ms()
        );
        assert_eq!(refresh_loop.status().failures, 0);
        assert_eq!(
            refresh_loop.status().last_success,
            Some(crate::StorageClusterRuntimeMapRefreshLoopSuccess {
                cluster_epoch: expected_runtime_map.cluster_epoch(),
                route_map_valid_until_ms: expected_runtime_map.valid_until_ms(),
            })
        );

        refresh_loop.stop();
        let attempts_after_stop = refresh_loop.status().attempts;
        std::thread::sleep(Duration::from_millis(15));
        assert_eq!(refresh_loop.status().attempts, attempts_after_stop);
    }

    #[test]
    fn storage_cluster_runtime_map_refresh_loop_rejects_zero_interval() {
        let tmp = test_util::tempdir();
        let store = FileControlPlaneStore::new(tmp.path().join("control-plane.state"));
        let mut authority = SingleAuthorityControlPlane::open(store).unwrap();
        authority
            .set_node_membership(NodeId::new(1), NodeMembershipState::Active)
            .unwrap();
        assert!(heartbeat_until_serving(&mut authority, 1, 1_000).serving());
        authority
            .set_pg_acting_set(PgId::new(31), vec![NodeId::new(1)])
            .unwrap();
        heartbeat_with_pg_observation(&mut authority, 1, 31, PgState::Peering, 1_001);
        let runtime_map = authority.snapshot().runtime_map(1_001).unwrap();
        let cluster = crate::StorageCluster::from_runtime_map(
            NodeId::new(1),
            &runtime_map,
            crate::EcShape { k: 1, m: 0 },
        )
        .unwrap();
        let handle = crate::StorageClusterRuntimeMapHandle::new(cluster);

        assert!(matches!(
            handle.spawn_control_plane_refresh_loop(authority, Duration::ZERO, || 1_000),
            Err(crate::cluster::StorageClusterRuntimeMapRefreshError::RefreshLoopZeroInterval)
        ));
    }

    #[test]
    fn runtime_node_routes_build_unix_storage_client_configs() {
        let tmp = test_util::tempdir();
        let store = FileControlPlaneStore::new(tmp.path().join("control-plane.state"));
        let mut authority = SingleAuthorityControlPlane::open(store).unwrap();
        authority
            .set_node_membership(NodeId::new(1), NodeMembershipState::Active)
            .unwrap();
        assert!(heartbeat_until_serving(&mut authority, 1, 1_000).serving());
        authority
            .set_pg_acting_set(PgId::new(30), vec![NodeId::new(1)])
            .unwrap();
        let runtime_map = authority.snapshot().runtime_map(1_001).unwrap();
        let node = &runtime_map.nodes()[0];

        let default_config =
            crate::cluster::LocalUnixStorageNodeClientConfig::from_runtime_node_route(node);
        assert_eq!(default_config.node_id(), NodeId::new(1));
        assert_eq!(
            default_config.socket_path(),
            std::path::Path::new("node-1.sock")
        );
        assert_eq!(
            default_config.rpc_admission_limit(),
            crate::cluster::LocalUnixStorageNodeClientConfig::DEFAULT_RPC_ADMISSION_LIMIT
        );

        let configured =
            crate::cluster::LocalUnixStorageNodeClientConfig::with_rpc_admission_from_runtime_node_route(
                node,
                17,
                std::time::Duration::from_millis(200),
                std::time::Duration::from_millis(300),
            );
        assert_eq!(configured.node_id(), NodeId::new(1));
        assert_eq!(
            configured.socket_path(),
            std::path::Path::new("node-1.sock")
        );
        assert_eq!(configured.rpc_admission_limit(), 17);
        assert_eq!(
            configured.rpc_admission_wait_timeout(),
            std::time::Duration::from_millis(200)
        );
        assert_eq!(
            configured.rpc_control_admission_wait_timeout(),
            std::time::Duration::from_millis(300)
        );

        let settings = crate::cluster::LocalUnixStorageNodeClientAdmissionSettings::new(
            23,
            std::time::Duration::from_millis(400),
            std::time::Duration::from_millis(500),
        );
        let [ref refreshed_config] =
            crate::StorageCluster::unix_storage_node_client_configs_from_runtime_map(
                &runtime_map,
                settings,
            )
            .try_into()
            .unwrap();
        assert_eq!(refreshed_config.node_id(), NodeId::new(1));
        assert_eq!(
            refreshed_config.socket_path(),
            std::path::Path::new("node-1.sock")
        );
        assert_eq!(refreshed_config.rpc_admission_limit(), 23);
        assert_eq!(
            refreshed_config.rpc_admission_wait_timeout(),
            std::time::Duration::from_millis(400)
        );
        assert_eq!(
            refreshed_config.rpc_control_admission_wait_timeout(),
            std::time::Duration::from_millis(500)
        );
    }

    #[test]
    fn runtime_map_installs_unix_storage_clients_from_absolute_endpoints() {
        let tmp = test_util::tempdir();
        let store = FileControlPlaneStore::new(tmp.path().join("control-plane.state"));
        let mut authority = SingleAuthorityControlPlane::open(store).unwrap();
        authority
            .set_node_membership(NodeId::new(1), NodeMembershipState::Active)
            .unwrap();
        let endpoint = tmp
            .path()
            .join("node-1.sock")
            .to_string_lossy()
            .into_owned();
        assert!(
            heartbeat_until_serving_with_endpoint(&mut authority, 1, 1_000, endpoint).serving()
        );
        authority
            .set_pg_acting_set(PgId::new(32), vec![NodeId::new(1)])
            .unwrap();
        let runtime_map = authority.snapshot().runtime_map(1_001).unwrap();

        let cluster = crate::StorageCluster::from_runtime_map_with_unix_storage_node_clients(
            NodeId::new(1),
            &runtime_map,
            crate::EcShape { k: 1, m: 0 },
        )
        .unwrap();

        assert_eq!(cluster.local_node_count(), 1);
        assert_eq!(
            cluster.local_node_ids().collect::<Vec<_>>(),
            vec![NodeId::new(1)]
        );
        assert_eq!(
            cluster.local_pg_route(PgId::new(32)).unwrap().state(),
            PgState::Peering
        );
    }

    #[test]
    fn runtime_map_builds_storage_node_process_config_for_node_routes() {
        let tmp = test_util::tempdir();
        let store = FileControlPlaneStore::new(tmp.path().join("control-plane.state"));
        let mut authority = SingleAuthorityControlPlane::open(store).unwrap();
        for node_id in [1, 2] {
            authority
                .set_node_membership(NodeId::new(node_id), NodeMembershipState::Active)
                .unwrap();
            assert!(heartbeat_until_serving(&mut authority, node_id, 1_000).serving());
        }
        authority
            .set_pg_acting_set(PgId::new(34), vec![NodeId::new(1), NodeId::new(2)])
            .unwrap();
        authority
            .set_pg_acting_set(PgId::new(35), vec![NodeId::new(2)])
            .unwrap();
        heartbeat_with_pg_observation(&mut authority, 1, 34, PgState::Peering, 1_001);
        heartbeat_with_pg_observation(&mut authority, 2, 34, PgState::Peering, 1_002);
        authority
            .complete_pg_peering(
                PgId::new(34),
                NodeId::new(1),
                node_incarnation(&authority, 1),
                1_003,
            )
            .unwrap();
        heartbeat_with_pg_observation(&mut authority, 1, 34, PgState::Active, 1_004);
        let runtime_map = authority.snapshot().runtime_map(1_005).unwrap();
        let valid_until_ms = runtime_map.valid_until_ms().unwrap();

        let node_1_config = crate::storage_node_server::StorageNodeProcessConfig::from_runtime_map(
            NodeId::new(1),
            tmp.path().join("node-1"),
            crate::EcShape { k: 1, m: 0 },
            &runtime_map,
        )
        .unwrap();
        assert_eq!(node_1_config.node_id, NodeId::new(1));
        assert_eq!(node_1_config.cluster_epoch, runtime_map.cluster_epoch());
        assert_eq!(
            node_1_config.route_map_valid_until_ms(),
            Some(valid_until_ms)
        );
        assert_eq!(
            node_1_config.socket_path,
            std::path::PathBuf::from("node-1.sock")
        );
        assert_eq!(node_1_config.pg_ids, vec![34]);
        assert_eq!(node_1_config.pg_routes.len(), 1);
        assert_eq!(node_1_config.pg_routes[0].pg_id, 34);
        assert_eq!(node_1_config.pg_routes[0].state, PgState::Active);
        assert_eq!(
            node_1_config.pg_routes[0].acting_set,
            vec![NodeId::new(1), NodeId::new(2)]
        );

        let node_2_config = crate::storage_node_server::StorageNodeProcessConfig::from_runtime_map(
            NodeId::new(2),
            tmp.path().join("node-2"),
            crate::EcShape { k: 1, m: 0 },
            &runtime_map,
        )
        .unwrap();
        assert_eq!(node_2_config.pg_ids, vec![34, 35]);
        assert_eq!(
            node_2_config.route_map_valid_until_ms(),
            Some(valid_until_ms)
        );
        assert_eq!(
            node_2_config
                .pg_routes
                .iter()
                .map(|route| route.pg_id)
                .collect::<Vec<_>>(),
            vec![34, 35]
        );
    }

    #[test]
    fn runtime_map_storage_node_config_rejects_absent_node() {
        let tmp = test_util::tempdir();
        let store = FileControlPlaneStore::new(tmp.path().join("control-plane.state"));
        let mut authority = SingleAuthorityControlPlane::open(store).unwrap();
        authority
            .set_node_membership(NodeId::new(1), NodeMembershipState::Active)
            .unwrap();
        assert!(heartbeat_until_serving(&mut authority, 1, 1_000).serving());
        authority
            .set_pg_acting_set(PgId::new(36), vec![NodeId::new(1)])
            .unwrap();
        let runtime_map = authority.snapshot().runtime_map(1_001).unwrap();

        assert!(matches!(
            crate::storage_node_server::StorageNodeProcessConfig::from_runtime_map(
                NodeId::new(2),
                tmp.path().join("node-2"),
                crate::EcShape { k: 1, m: 0 },
                &runtime_map,
            ),
            Err(crate::storage_node_server::StorageNodeServerError::RuntimeMapNodeNotFound {
                node_id: 2,
                cluster_epoch,
            }) if cluster_epoch == runtime_map.cluster_epoch()
        ));
    }

    #[test]
    fn runtime_map_unix_storage_clients_reject_relative_endpoints() {
        let tmp = test_util::tempdir();
        let store = FileControlPlaneStore::new(tmp.path().join("control-plane.state"));
        let mut authority = SingleAuthorityControlPlane::open(store).unwrap();
        authority
            .set_node_membership(NodeId::new(1), NodeMembershipState::Active)
            .unwrap();
        assert!(heartbeat_until_serving(&mut authority, 1, 1_000).serving());
        authority
            .set_pg_acting_set(PgId::new(33), vec![NodeId::new(1)])
            .unwrap();
        let runtime_map = authority.snapshot().runtime_map(1_001).unwrap();

        assert!(matches!(
            crate::StorageCluster::from_runtime_map_with_unix_storage_node_clients(
                NodeId::new(1),
                &runtime_map,
                crate::EcShape { k: 1, m: 0 },
            ),
            Err(crate::ClusterBuildError::RemoteStorageNodeClientSocketPathNotAbsolute { path })
                if path.as_path() == std::path::Path::new("node-1.sock")
        ));
    }

    #[test]
    fn runtime_map_requires_endpoints_for_routed_nodes() {
        let tmp = test_util::tempdir();
        let store = FileControlPlaneStore::new(tmp.path().join("control-plane.state"));
        let mut authority = SingleAuthorityControlPlane::open(store).unwrap();
        authority
            .set_node_membership(NodeId::new(1), NodeMembershipState::Active)
            .unwrap();
        authority
            .set_pg_acting_set(PgId::new(26), vec![NodeId::new(1)])
            .unwrap();

        assert!(matches!(
            authority.snapshot().runtime_map(1_000),
            Err(ControlPlaneError::NodeEndpointMissing { node_id: 1, .. })
        ));
    }

    #[test]
    fn active_pg_route_fails_closed_after_primary_lease_expiry() {
        let tmp = test_util::tempdir();
        let store = FileControlPlaneStore::new(tmp.path().join("control-plane.state"));
        let mut authority = SingleAuthorityControlPlane::open(store).unwrap();
        authority
            .set_node_membership(NodeId::new(1), NodeMembershipState::Active)
            .unwrap();
        assert!(heartbeat_until_serving(&mut authority, 1, 1_000).serving());
        authority
            .set_pg_acting_set(PgId::new(23), vec![NodeId::new(1)])
            .unwrap();
        heartbeat_with_pg_observation(&mut authority, 1, 23, PgState::Peering, 2_000);
        authority
            .complete_pg_peering(
                PgId::new(23),
                NodeId::new(1),
                node_incarnation(&authority, 1),
                2_001,
            )
            .unwrap();
        heartbeat_with_pg_observation(&mut authority, 1, 23, PgState::Active, 2_002);
        let lease_deadline = authority
            .snapshot()
            .node(NodeId::new(1))
            .unwrap()
            .lease_deadline_ms()
            .unwrap();

        assert!(matches!(
            authority
                .snapshot()
                .active_pg_route(PgId::new(23), lease_deadline),
            Err(ControlPlaneError::PgHasNoServingPrimary { pg_id: 23, .. })
        ));
    }

    #[test]
    fn complete_pg_peering_requires_unexpired_primary_lease() {
        let tmp = test_util::tempdir();
        let store = FileControlPlaneStore::new(tmp.path().join("control-plane.state"));
        let mut authority = SingleAuthorityControlPlane::open(store).unwrap();
        authority
            .set_node_membership(NodeId::new(1), NodeMembershipState::Active)
            .unwrap();
        assert!(heartbeat_until_serving(&mut authority, 1, 1_000).serving());
        authority
            .set_pg_acting_set(PgId::new(11), vec![NodeId::new(1)])
            .unwrap();
        heartbeat_with_pg_observation(&mut authority, 1, 11, PgState::Peering, 1_050);
        let lease_deadline = authority
            .snapshot()
            .node(NodeId::new(1))
            .unwrap()
            .lease_deadline_ms()
            .unwrap();
        assert!(matches!(
            authority.complete_pg_peering(
                PgId::new(11),
                NodeId::new(1),
                node_incarnation(&authority, 1),
                lease_deadline,
            ),
            Err(ControlPlaneError::NodeLeaseExpired { node_id: 1, .. })
        ));
        assert_eq!(
            authority.snapshot().pg(PgId::new(11)).unwrap().state(),
            PgState::Peering
        );

        heartbeat_with_pg_observation(&mut authority, 1, 11, PgState::Peering, lease_deadline + 1);
        authority
            .complete_pg_peering(
                PgId::new(11),
                NodeId::new(1),
                node_incarnation(&authority, 1),
                lease_deadline + 2,
            )
            .unwrap();
    }

    #[test]
    fn complete_pg_peering_requires_current_peering_observation() {
        let tmp = test_util::tempdir();
        let store = FileControlPlaneStore::new(tmp.path().join("control-plane.state"));
        let mut authority = SingleAuthorityControlPlane::open(store).unwrap();
        authority
            .set_node_membership(NodeId::new(1), NodeMembershipState::Active)
            .unwrap();
        assert!(heartbeat_until_serving(&mut authority, 1, 1_000).serving());
        authority
            .set_pg_acting_set(PgId::new(12), vec![NodeId::new(1)])
            .unwrap();
        authority
            .heartbeat(
                heartbeat_from_record(&authority, 1, authority.snapshot().cluster_epoch(), 2_000),
                2_000,
            )
            .unwrap();

        assert!(matches!(
            authority.complete_pg_peering(
                PgId::new(12),
                NodeId::new(1),
                node_incarnation(&authority, 1),
                2_010,
            ),
            Err(ControlPlaneError::PgPeeringMissingObservation {
                pg_id: 12,
                node_id: 1,
                ..
            })
        ));

        heartbeat_with_pg_observation(&mut authority, 1, 12, PgState::Active, 2_020);
        assert!(matches!(
            authority.complete_pg_peering(
                PgId::new(12),
                NodeId::new(1),
                node_incarnation(&authority, 1),
                2_030,
            ),
            Err(ControlPlaneError::PgPeeringObservationNotPeering {
                pg_id: 12,
                node_id: 1,
                state: PgState::Active,
                ..
            })
        ));

        heartbeat_with_pg_observation(&mut authority, 1, 12, PgState::Peering, 2_040);
        authority
            .complete_pg_peering(
                PgId::new(12),
                NodeId::new(1),
                node_incarnation(&authority, 1),
                2_050,
            )
            .unwrap();
    }

    #[test]
    fn complete_pg_peering_only_activates_from_peering() {
        let tmp = test_util::tempdir();
        let store = FileControlPlaneStore::new(tmp.path().join("control-plane.state"));
        let mut authority = SingleAuthorityControlPlane::open(store).unwrap();
        authority
            .set_node_membership(NodeId::new(1), NodeMembershipState::Active)
            .unwrap();
        assert!(heartbeat_until_serving(&mut authority, 1, 1_000).serving());
        authority
            .set_pg_acting_set(PgId::new(15), vec![NodeId::new(1)])
            .unwrap();
        authority
            .set_pg_state(PgId::new(15), PgState::Degraded)
            .unwrap();
        authority
            .heartbeat(
                heartbeat_from_record(&authority, 1, authority.snapshot().cluster_epoch(), 2_000),
                2_000,
            )
            .unwrap();

        assert!(matches!(
            authority.complete_pg_peering(
                PgId::new(15),
                NodeId::new(1),
                node_incarnation(&authority, 1),
                2_010,
            ),
            Err(ControlPlaneError::PgNotPeering {
                pg_id: 15,
                state: PgState::Degraded,
                ..
            })
        ));
    }

    #[test]
    fn active_pg_service_requires_primary_active_observation() {
        let tmp = test_util::tempdir();
        let store = FileControlPlaneStore::new(tmp.path().join("control-plane.state"));
        let mut authority = SingleAuthorityControlPlane::open(store).unwrap();
        authority
            .set_node_membership(NodeId::new(1), NodeMembershipState::Active)
            .unwrap();
        assert!(heartbeat_until_serving(&mut authority, 1, 1_000).serving());
        authority
            .set_pg_acting_set(PgId::new(16), vec![NodeId::new(1)])
            .unwrap();
        heartbeat_with_pg_observation(&mut authority, 1, 16, PgState::Peering, 2_000);
        authority
            .complete_pg_peering(
                PgId::new(16),
                NodeId::new(1),
                node_incarnation(&authority, 1),
                2_010,
            )
            .unwrap();

        assert_eq!(authority.serving_pg_primary(PgId::new(16), 2_010), None);
        authority
            .heartbeat(
                heartbeat_from_record(&authority, 1, authority.snapshot().cluster_epoch(), 2_020),
                2_020,
            )
            .unwrap();
        assert!(matches!(
            authority.authorize_pg_primary_service(
                PgId::new(16),
                NodeId::new(1),
                node_incarnation(&authority, 1),
                authority.snapshot().cluster_epoch(),
                2_021,
            ),
            Err(ControlPlaneError::PgPrimaryMissingActiveObservation {
                pg_id: 16,
                node_id: 1,
                ..
            })
        ));

        heartbeat_with_pg_observation(&mut authority, 1, 16, PgState::Peering, 2_030);
        assert_eq!(authority.serving_pg_primary(PgId::new(16), 2_030), None);
        assert!(matches!(
            authority.authorize_pg_primary_service(
                PgId::new(16),
                NodeId::new(1),
                node_incarnation(&authority, 1),
                authority.snapshot().cluster_epoch(),
                2_040,
            ),
            Err(ControlPlaneError::PgPrimaryObservationNotActive {
                pg_id: 16,
                node_id: 1,
                state: PgState::Peering,
                ..
            })
        ));

        heartbeat_with_pg_observation(&mut authority, 1, 16, PgState::Active, 2_050);
        assert_eq!(
            authority.serving_pg_primary(PgId::new(16), 2_050),
            Some(NodeId::new(1))
        );
        authority
            .authorize_pg_primary_service(
                PgId::new(16),
                NodeId::new(1),
                node_incarnation(&authority, 1),
                authority.snapshot().cluster_epoch(),
                2_060,
            )
            .unwrap();
    }

    #[test]
    fn node_service_authorization_requires_current_epoch_incarnation_and_lease() {
        let tmp = test_util::tempdir();
        let store = FileControlPlaneStore::new(tmp.path().join("control-plane.state"));
        let mut authority = SingleAuthorityControlPlane::open(store).unwrap();
        authority
            .set_node_membership(NodeId::new(1), NodeMembershipState::Active)
            .unwrap();
        let serving = heartbeat_until_serving(&mut authority, 1, 100);
        assert!(serving.serving());
        let record = authority.snapshot().node(NodeId::new(1)).unwrap();
        let node_incarnation = record.node_incarnation();
        let lease_deadline_ms = record.lease_deadline_ms().unwrap();

        let authorized = authority
            .authorize_node_service(
                NodeId::new(1),
                node_incarnation,
                serving.cluster_epoch(),
                150,
            )
            .unwrap();
        assert_eq!(authorized.node_id(), NodeId::new(1));
        assert_eq!(authorized.node_incarnation(), node_incarnation);
        assert_eq!(authorized.cluster_epoch(), serving.cluster_epoch());
        assert_eq!(
            authorized.authority_incarnation(),
            authority.snapshot().authority_incarnation()
        );
        assert_eq!(authorized.lease_deadline_ms(), lease_deadline_ms);
        authority
            .validate_node_service_authorization(&authorized, 151)
            .unwrap();

        assert!(matches!(
            authority.authorize_node_service(
                NodeId::new(1),
                node_incarnation + 1,
                serving.cluster_epoch(),
                150,
            ),
            Err(ControlPlaneError::NodeIncarnationMismatch { node_id: 1, .. })
        ));
        assert!(matches!(
            authority.authorize_node_service(
                NodeId::new(1),
                node_incarnation,
                ClusterEpoch::INITIAL,
                150,
            ),
            Err(ControlPlaneError::StaleNodeObservedEpoch { node_id: 1, .. })
        ));
        assert!(matches!(
            authority.authorize_node_service(
                NodeId::new(1),
                node_incarnation,
                serving.cluster_epoch(),
                lease_deadline_ms,
            ),
            Err(ControlPlaneError::NodeLeaseExpired { node_id: 1, .. })
        ));
        assert!(matches!(
            authority.validate_node_service_authorization(&authorized, lease_deadline_ms),
            Err(ControlPlaneError::NodeLeaseExpired { node_id: 1, .. })
        ));

        let mut shorter_lease =
            heartbeat_from_record(&authority, 1, authority.snapshot().cluster_epoch(), 152);
        shorter_lease.requested_lease_duration_ms = 5;
        authority.heartbeat(shorter_lease, 152).unwrap();
        assert!(authorized.lease_deadline_ms() > 157);
        assert!(matches!(
            authority.validate_node_service_authorization(&authorized, 157),
            Err(ControlPlaneError::NodeLeaseExpired {
                node_id: 1,
                lease_deadline_ms: Some(157),
                ..
            })
        ));
    }

    #[test]
    fn pg_primary_authorization_fails_closed_for_peering_and_wrong_primary() {
        let tmp = test_util::tempdir();
        let store = FileControlPlaneStore::new(tmp.path().join("control-plane.state"));
        let mut authority = SingleAuthorityControlPlane::open(store).unwrap();
        for node_id in [1, 2] {
            authority
                .set_node_membership(NodeId::new(node_id), NodeMembershipState::Active)
                .unwrap();
            assert!(heartbeat_until_serving(&mut authority, node_id, 1_000).serving());
        }
        authority
            .heartbeat(
                heartbeat_from_record(&authority, 1, authority.snapshot().cluster_epoch(), 1_002),
                1_002,
            )
            .unwrap();
        authority
            .set_pg_acting_set(PgId::new(10), vec![NodeId::new(1), NodeId::new(2)])
            .unwrap();
        for node_id in [1, 2] {
            heartbeat_with_pg_observation(
                &mut authority,
                node_id,
                10,
                PgState::Peering,
                2_000 + u64::from(node_id),
            );
        }
        let node_one_incarnation = authority
            .snapshot()
            .node(NodeId::new(1))
            .unwrap()
            .node_incarnation();
        assert!(matches!(
            authority.authorize_pg_primary_service(
                PgId::new(10),
                NodeId::new(1),
                node_one_incarnation,
                authority.snapshot().cluster_epoch(),
                2_050,
            ),
            Err(ControlPlaneError::PgNotActive { pg_id: 10, .. })
        ));

        authority
            .complete_pg_peering(
                PgId::new(10),
                NodeId::new(1),
                node_incarnation(&authority, 1),
                2_050,
            )
            .unwrap();
        heartbeat_with_pg_observation(&mut authority, 1, 10, PgState::Active, 3_001);
        authority
            .heartbeat(
                heartbeat_from_record(&authority, 2, authority.snapshot().cluster_epoch(), 3_002),
                3_002,
            )
            .unwrap();
        let node_one_incarnation = authority
            .snapshot()
            .node(NodeId::new(1))
            .unwrap()
            .node_incarnation();
        let node_two_incarnation = authority
            .snapshot()
            .node(NodeId::new(2))
            .unwrap()
            .node_incarnation();
        let authorized = authority
            .authorize_pg_primary_service(
                PgId::new(10),
                NodeId::new(1),
                node_one_incarnation,
                authority.snapshot().cluster_epoch(),
                3_050,
            )
            .unwrap();
        assert_eq!(authorized.pg_id(), PgId::new(10));
        assert_eq!(authorized.primary_node_id(), NodeId::new(1));
        assert_eq!(
            authorized.cluster_epoch(),
            authority.snapshot().cluster_epoch()
        );
        assert_eq!(
            authorized.authority_incarnation(),
            authority.snapshot().authority_incarnation()
        );
        assert_eq!(
            authorized.lease_deadline_ms(),
            authority
                .snapshot()
                .node(NodeId::new(1))
                .unwrap()
                .lease_deadline_ms()
                .unwrap()
        );

        assert!(matches!(
            authority.authorize_pg_primary_service(
                PgId::new(10),
                NodeId::new(2),
                node_two_incarnation,
                authority.snapshot().cluster_epoch(),
                3_050,
            ),
            Err(ControlPlaneError::NodeNotPgPrimary {
                pg_id: 10,
                node_id: 2,
                primary_node_id: 1,
                ..
            })
        ));
    }

    #[test]
    fn pg_operation_authorization_requires_active_primary_for_all_operation_classes() {
        let tmp = test_util::tempdir();
        let store = FileControlPlaneStore::new(tmp.path().join("control-plane.state"));
        let mut authority = SingleAuthorityControlPlane::open(store).unwrap();
        authority
            .set_node_membership(NodeId::new(1), NodeMembershipState::Active)
            .unwrap();
        assert!(heartbeat_until_serving(&mut authority, 1, 1_000).serving());
        authority
            .set_pg_acting_set(PgId::new(14), vec![NodeId::new(1)])
            .unwrap();
        heartbeat_with_pg_observation(&mut authority, 1, 14, PgState::Peering, 2_000);
        authority
            .complete_pg_peering(
                PgId::new(14),
                NodeId::new(1),
                node_incarnation(&authority, 1),
                2_050,
            )
            .unwrap();
        heartbeat_with_pg_observation(&mut authority, 1, 14, PgState::Active, 3_000);

        let operations = [
            PgServiceOperation::MetadataRead,
            PgServiceOperation::MetadataList,
            PgServiceOperation::MetadataWrite,
            PgServiceOperation::PayloadRead,
            PgServiceOperation::PayloadWrite,
        ];
        for operation in operations {
            let authorization = authority
                .authorize_pg_operation(
                    operation,
                    PgId::new(14),
                    NodeId::new(1),
                    node_incarnation(&authority, 1),
                    authority.snapshot().cluster_epoch(),
                    3_050,
                )
                .unwrap();
            assert_eq!(authorization.operation(), operation);
            assert_eq!(authorization.pg_id(), PgId::new(14));
            assert_eq!(authorization.primary_node_id(), NodeId::new(1));
            assert_eq!(
                authorization.primary_node_incarnation(),
                node_incarnation(&authority, 1)
            );
            assert_eq!(
                authorization.cluster_epoch(),
                authority.snapshot().cluster_epoch()
            );
            authority
                .validate_pg_operation_authorization(&authorization, 3_060)
                .unwrap();
            authority
                .validate_pg_operation_authorization_for(&authorization, operation, 3_060)
                .unwrap();
            let wrong_operation = match operation {
                PgServiceOperation::MetadataWrite => PgServiceOperation::MetadataRead,
                _ => PgServiceOperation::MetadataWrite,
            };
            assert!(matches!(
                authority.validate_pg_operation_authorization_for(
                    &authorization,
                    wrong_operation,
                    3_060,
                ),
                Err(ControlPlaneError::PgOperationAuthorizationMismatch {
                    expected,
                    actual,
                }) if expected == wrong_operation && actual == operation
            ));
        }

        for state in [
            PgState::Peering,
            PgState::Degraded,
            PgState::Backfilling,
            PgState::Inconsistent,
        ] {
            authority.set_pg_state(PgId::new(14), state).unwrap();
            authority
                .heartbeat(
                    heartbeat_from_record(
                        &authority,
                        1,
                        authority.snapshot().cluster_epoch(),
                        4_000,
                    ),
                    4_000,
                )
                .unwrap();
            for operation in operations {
                assert!(matches!(
                    authority.authorize_pg_operation(
                        operation,
                        PgId::new(14),
                        NodeId::new(1),
                        node_incarnation(&authority, 1),
                        authority.snapshot().cluster_epoch(),
                        4_050,
                    ),
                    Err(ControlPlaneError::PgNotActive {
                        pg_id: 14,
                        state: err_state,
                        ..
                    }) if err_state == state
                ));
            }
        }
    }

    #[test]
    fn pg_operation_authorization_validation_fences_stale_tokens() {
        let tmp = test_util::tempdir();
        let store = FileControlPlaneStore::new(tmp.path().join("control-plane.state"));
        let mut authority = SingleAuthorityControlPlane::open(store.clone()).unwrap();
        authority
            .set_node_membership(NodeId::new(1), NodeMembershipState::Active)
            .unwrap();
        assert!(heartbeat_until_serving(&mut authority, 1, 1_000).serving());
        authority
            .set_pg_acting_set(PgId::new(17), vec![NodeId::new(1)])
            .unwrap();
        heartbeat_with_pg_observation(&mut authority, 1, 17, PgState::Peering, 2_000);
        authority
            .complete_pg_peering(
                PgId::new(17),
                NodeId::new(1),
                node_incarnation(&authority, 1),
                2_050,
            )
            .unwrap();
        heartbeat_with_pg_observation(&mut authority, 1, 17, PgState::Active, 3_000);

        let authorization = authority
            .authorize_pg_operation(
                PgServiceOperation::MetadataWrite,
                PgId::new(17),
                NodeId::new(1),
                node_incarnation(&authority, 1),
                authority.snapshot().cluster_epoch(),
                3_050,
            )
            .unwrap();
        authority
            .validate_pg_operation_authorization(&authorization, 3_060)
            .unwrap();

        let mut shorter_lease =
            heartbeat_from_record(&authority, 1, authority.snapshot().cluster_epoch(), 3_061);
        shorter_lease.requested_lease_duration_ms = 5;
        shorter_lease.pg_observations = vec![NodePgHeartbeatObservation {
            pg_id: PgId::new(17),
            state: PgState::Active,
            metadata_proof: PgMetadataProof::empty(),
        }];
        authority.heartbeat(shorter_lease, 3_061).unwrap();
        assert!(authorization.lease_deadline_ms() > 3_066);
        assert!(matches!(
            authority.validate_pg_operation_authorization(&authorization, 3_066),
            Err(ControlPlaneError::NodeLeaseExpired {
                node_id: 1,
                lease_deadline_ms: Some(3_066),
                ..
            })
        ));

        assert!(matches!(
            authority.validate_pg_operation_authorization(
                &authorization,
                authorization.lease_deadline_ms()
            ),
            Err(ControlPlaneError::NodeLeaseExpired { node_id: 1, .. })
        ));

        heartbeat_with_pg_observation(&mut authority, 1, 17, PgState::Active, 3_070);
        authority
            .set_pg_state(PgId::new(17), PgState::Peering)
            .unwrap();
        assert!(matches!(
            authority.validate_pg_operation_authorization(&authorization, 3_060),
            Err(ControlPlaneError::StaleAuthorizationEpoch { .. })
        ));

        let restarted = SingleAuthorityControlPlane::open(store).unwrap();
        assert!(matches!(
            restarted.validate_pg_operation_authorization(&authorization, 3_060),
            Err(ControlPlaneError::StaleAuthorityIncarnation { .. })
        ));
    }

    #[test]
    fn stale_observed_epoch_heartbeat_returns_map_without_becoming_serving() {
        let tmp = test_util::tempdir();
        let store = FileControlPlaneStore::new(tmp.path().join("control-plane.state"));
        let mut authority = SingleAuthorityControlPlane::open(store).unwrap();
        authority
            .set_node_membership(NodeId::new(6), NodeMembershipState::Active)
            .unwrap();
        let stale_epoch = ClusterEpoch::INITIAL;
        let lease = authority
            .heartbeat(heartbeat(6, stale_epoch, 100), 100)
            .unwrap();
        assert!(!lease.serving());
        assert_eq!(lease.snapshot().cluster_epoch(), lease.cluster_epoch());
        assert_eq!(
            authority.deterministic_pg_primary(PgId::new(1), &[NodeId::new(6)], 100),
            None
        );

        let caught_up = authority
            .heartbeat(heartbeat(6, lease.cluster_epoch(), 200), 200)
            .unwrap();
        assert!(!caught_up.serving());
        let final_lease = authority
            .heartbeat(heartbeat(6, caught_up.cluster_epoch(), 300), 300)
            .unwrap();
        assert!(final_lease.serving());
    }

    #[test]
    fn heartbeat_records_current_epoch_pg_observations() {
        let tmp = test_util::tempdir();
        let store = FileControlPlaneStore::new(tmp.path().join("control-plane.state"));
        let mut authority = SingleAuthorityControlPlane::open(store.clone()).unwrap();
        authority
            .set_node_membership(NodeId::new(1), NodeMembershipState::Active)
            .unwrap();
        assert!(heartbeat_until_serving(&mut authority, 1, 1_000).serving());
        authority
            .set_pg_acting_set(PgId::new(15), vec![NodeId::new(1)])
            .unwrap();

        let mut heartbeat =
            heartbeat_from_record(&authority, 1, authority.snapshot().cluster_epoch(), 2_000);
        heartbeat.pg_observations = vec![NodePgHeartbeatObservation {
            pg_id: PgId::new(15),
            state: PgState::Peering,
            metadata_proof: PgMetadataProof {
                applied_log_index: 9,
                applied_log_hash: 10,
                state_digest: 11,
            },
        }];
        authority.heartbeat(heartbeat, 2_000).unwrap();

        let observation = authority
            .snapshot()
            .node(NodeId::new(1))
            .unwrap()
            .pg_observation(PgId::new(15))
            .unwrap();
        assert_eq!(observation.pg_id(), PgId::new(15));
        assert_eq!(observation.state(), PgState::Peering);
        assert_eq!(
            observation.observed_epoch(),
            authority.snapshot().cluster_epoch()
        );
        assert_eq!(observation.observed_at_ms(), 2_000);
        assert_eq!(
            observation.metadata_proof(),
            PgMetadataProof {
                applied_log_index: 9,
                applied_log_hash: 10,
                state_digest: 11,
            }
        );
        let persisted = store.load().unwrap().unwrap();
        let persisted_observation = persisted
            .node(NodeId::new(1))
            .unwrap()
            .pg_observation(PgId::new(15))
            .unwrap();
        assert_eq!(persisted_observation.state(), PgState::Peering);
        assert_eq!(
            persisted_observation.metadata_proof(),
            PgMetadataProof {
                applied_log_index: 9,
                applied_log_hash: 10,
                state_digest: 11,
            }
        );
    }

    #[test]
    fn complete_pg_peering_requires_matching_metadata_proofs() {
        let tmp = test_util::tempdir();
        let store = FileControlPlaneStore::new(tmp.path().join("control-plane.state"));
        let mut authority = SingleAuthorityControlPlane::open(store.clone()).unwrap();
        for node_id in [1, 2] {
            authority
                .set_node_membership(NodeId::new(node_id), NodeMembershipState::Active)
                .unwrap();
            assert!(heartbeat_until_serving(&mut authority, node_id, 1_000).serving());
        }
        authority
            .set_pg_acting_set(PgId::new(19), vec![NodeId::new(1), NodeId::new(2)])
            .unwrap();

        let matching_proof = PgMetadataProof {
            applied_log_index: 42,
            applied_log_hash: 0xabc,
            state_digest: 0xdef,
        };
        let different_proof = PgMetadataProof {
            applied_log_index: 41,
            applied_log_hash: 0xabc,
            state_digest: 0xdef,
        };
        for (node_id, metadata_proof) in [(1, matching_proof), (2, different_proof)] {
            let mut heartbeat = heartbeat_from_record(
                &authority,
                node_id,
                authority.snapshot().cluster_epoch(),
                2_000 + u64::from(node_id),
            );
            heartbeat.pg_observations = vec![NodePgHeartbeatObservation {
                pg_id: PgId::new(19),
                state: PgState::Peering,
                metadata_proof,
            }];
            authority
                .heartbeat(heartbeat, 2_000 + u64::from(node_id))
                .unwrap();
        }
        assert!(matches!(
            authority.complete_pg_peering(
                PgId::new(19),
                NodeId::new(1),
                node_incarnation(&authority, 1),
                2_050,
            ),
            Err(ControlPlaneError::PgPeeringMetadataProofMismatch {
                pg_id: 19,
                node_id: 2,
                expected,
                actual,
                ..
            }) if expected == matching_proof && actual == different_proof
        ));

        let mut heartbeat =
            heartbeat_from_record(&authority, 2, authority.snapshot().cluster_epoch(), 2_060);
        heartbeat.pg_observations = vec![NodePgHeartbeatObservation {
            pg_id: PgId::new(19),
            state: PgState::Peering,
            metadata_proof: matching_proof,
        }];
        authority.heartbeat(heartbeat, 2_060).unwrap();
        authority
            .complete_pg_peering(
                PgId::new(19),
                NodeId::new(1),
                node_incarnation(&authority, 1),
                2_070,
            )
            .unwrap();
        let active_pg = authority.snapshot().pg(PgId::new(19)).unwrap();
        assert_eq!(active_pg.active_primary(), Some(NodeId::new(1)));
        assert_eq!(active_pg.active_metadata_proof(), Some(matching_proof));
        let persisted = store.load().unwrap().unwrap();
        let persisted_pg = persisted.pg(PgId::new(19)).unwrap();
        assert_eq!(persisted_pg.active_primary(), Some(NodeId::new(1)));
        assert_eq!(persisted_pg.active_metadata_proof(), Some(matching_proof));
    }

    #[test]
    fn active_heartbeat_must_match_accepted_peering_metadata_proof() {
        let tmp = test_util::tempdir();
        let store = FileControlPlaneStore::new(tmp.path().join("control-plane.state"));
        let mut authority = SingleAuthorityControlPlane::open(store).unwrap();
        authority
            .set_node_membership(NodeId::new(1), NodeMembershipState::Active)
            .unwrap();
        assert!(heartbeat_until_serving(&mut authority, 1, 1_000).serving());
        authority
            .set_pg_acting_set(PgId::new(19), vec![NodeId::new(1)])
            .unwrap();

        let accepted_proof = PgMetadataProof {
            applied_log_index: 42,
            applied_log_hash: 0xabc,
            state_digest: 0xdef,
        };
        let mut peering_heartbeat =
            heartbeat_from_record(&authority, 1, authority.snapshot().cluster_epoch(), 2_000);
        peering_heartbeat.pg_observations = vec![NodePgHeartbeatObservation {
            pg_id: PgId::new(19),
            state: PgState::Peering,
            metadata_proof: accepted_proof,
        }];
        authority.heartbeat(peering_heartbeat, 2_000).unwrap();
        authority
            .complete_pg_peering(
                PgId::new(19),
                NodeId::new(1),
                node_incarnation(&authority, 1),
                2_010,
            )
            .unwrap();

        let different_proof = PgMetadataProof {
            applied_log_index: 43,
            applied_log_hash: 0xabc,
            state_digest: 0xdef,
        };
        let mut mismatched_active =
            heartbeat_from_record(&authority, 1, authority.snapshot().cluster_epoch(), 2_020);
        mismatched_active.pg_observations = vec![NodePgHeartbeatObservation {
            pg_id: PgId::new(19),
            state: PgState::Active,
            metadata_proof: different_proof,
        }];
        assert!(matches!(
            authority.heartbeat(mismatched_active, 2_020),
            Err(ControlPlaneError::PgActiveMetadataProofMismatch {
                pg_id: 19,
                node_id: 1,
                expected,
                actual,
                ..
            }) if expected == accepted_proof && actual == different_proof
        ));
        assert!(authority
            .snapshot()
            .node(NodeId::new(1))
            .unwrap()
            .pg_observation(PgId::new(19))
            .is_none());

        let mut matching_active =
            heartbeat_from_record(&authority, 1, authority.snapshot().cluster_epoch(), 2_030);
        matching_active.pg_observations = vec![NodePgHeartbeatObservation {
            pg_id: PgId::new(19),
            state: PgState::Active,
            metadata_proof: accepted_proof,
        }];
        authority.heartbeat(matching_active, 2_030).unwrap();
        let observation = authority
            .snapshot()
            .node(NodeId::new(1))
            .unwrap()
            .pg_observation(PgId::new(19))
            .unwrap();
        assert_eq!(observation.state(), PgState::Active);
        assert_eq!(observation.metadata_proof(), accepted_proof);
    }

    #[test]
    fn stale_heartbeat_does_not_mutate_pg_observations() {
        let tmp = test_util::tempdir();
        let store = FileControlPlaneStore::new(tmp.path().join("control-plane.state"));
        let mut authority = SingleAuthorityControlPlane::open(store).unwrap();
        authority
            .set_node_membership(NodeId::new(1), NodeMembershipState::Active)
            .unwrap();
        assert!(heartbeat_until_serving(&mut authority, 1, 1_000).serving());
        authority
            .set_pg_acting_set(PgId::new(16), vec![NodeId::new(1)])
            .unwrap();
        let current_epoch = authority.snapshot().cluster_epoch();

        let mut stale = heartbeat_from_record(
            &authority,
            1,
            ClusterEpoch::new(current_epoch.get() - 1).unwrap(),
            2_000,
        );
        stale.pg_observations = vec![NodePgHeartbeatObservation {
            pg_id: PgId::new(16),
            state: PgState::Active,
            metadata_proof: PgMetadataProof::empty(),
        }];
        let response = authority.heartbeat(stale, 2_000).unwrap();
        assert!(!response.serving());
        assert!(authority
            .snapshot()
            .node(NodeId::new(1))
            .unwrap()
            .pg_observation(PgId::new(16))
            .is_none());
    }

    #[test]
    fn heartbeat_rejects_invalid_pg_observations() {
        let tmp = test_util::tempdir();
        let store = FileControlPlaneStore::new(tmp.path().join("control-plane.state"));
        let mut authority = SingleAuthorityControlPlane::open(store).unwrap();
        for node_id in [1, 2] {
            authority
                .set_node_membership(NodeId::new(node_id), NodeMembershipState::Active)
                .unwrap();
            assert!(heartbeat_until_serving(&mut authority, node_id, 1_000).serving());
        }
        authority
            .set_pg_acting_set(PgId::new(17), vec![NodeId::new(1)])
            .unwrap();

        let mut duplicate =
            heartbeat_from_record(&authority, 1, authority.snapshot().cluster_epoch(), 2_000);
        duplicate.pg_observations = vec![
            NodePgHeartbeatObservation {
                pg_id: PgId::new(17),
                state: PgState::Peering,
                metadata_proof: PgMetadataProof::empty(),
            },
            NodePgHeartbeatObservation {
                pg_id: PgId::new(17),
                state: PgState::Peering,
                metadata_proof: PgMetadataProof::empty(),
            },
        ];
        assert!(matches!(
            authority.heartbeat(duplicate, 2_000),
            Err(ControlPlaneError::DuplicatePgObservation {
                node_id: 1,
                pg_id: 17
            })
        ));

        let mut unknown =
            heartbeat_from_record(&authority, 1, authority.snapshot().cluster_epoch(), 2_001);
        unknown.pg_observations = vec![NodePgHeartbeatObservation {
            pg_id: PgId::new(99),
            state: PgState::Peering,
            metadata_proof: PgMetadataProof::empty(),
        }];
        assert!(matches!(
            authority.heartbeat(unknown, 2_001),
            Err(ControlPlaneError::UnknownPg { pg_id: 99 })
        ));

        let mut wrong_node =
            heartbeat_from_record(&authority, 2, authority.snapshot().cluster_epoch(), 2_002);
        wrong_node.pg_observations = vec![NodePgHeartbeatObservation {
            pg_id: PgId::new(17),
            state: PgState::Peering,
            metadata_proof: PgMetadataProof::empty(),
        }];
        assert!(matches!(
            authority.heartbeat(wrong_node, 2_002),
            Err(ControlPlaneError::PgObservationNotInActingSet {
                node_id: 2,
                pg_id: 17
            })
        ));
    }

    #[test]
    fn epoch_change_clears_current_pg_observations_and_preserves_history() {
        let tmp = test_util::tempdir();
        let store = FileControlPlaneStore::new(tmp.path().join("control-plane.state"));
        let mut authority = SingleAuthorityControlPlane::open(store).unwrap();
        authority
            .set_node_membership(NodeId::new(1), NodeMembershipState::Active)
            .unwrap();
        assert!(heartbeat_until_serving(&mut authority, 1, 1_000).serving());
        authority
            .set_pg_acting_set(PgId::new(18), vec![NodeId::new(1)])
            .unwrap();
        let observation_epoch = authority.snapshot().cluster_epoch();
        let mut heartbeat = heartbeat_from_record(&authority, 1, observation_epoch, 2_000);
        heartbeat.pg_observations = vec![NodePgHeartbeatObservation {
            pg_id: PgId::new(18),
            state: PgState::Peering,
            metadata_proof: PgMetadataProof::empty(),
        }];
        authority.heartbeat(heartbeat, 2_000).unwrap();
        assert!(authority
            .snapshot()
            .node(NodeId::new(1))
            .unwrap()
            .pg_observation(PgId::new(18))
            .is_some());

        authority
            .set_node_membership(NodeId::new(2), NodeMembershipState::Active)
            .unwrap();
        assert!(authority
            .snapshot()
            .node(NodeId::new(1))
            .unwrap()
            .pg_observation(PgId::new(18))
            .is_none());
        let history = authority
            .snapshot()
            .cluster_map_at_epoch(observation_epoch)
            .unwrap();
        let historical_node = history
            .nodes()
            .iter()
            .find(|record| record.node_id() == NodeId::new(1))
            .unwrap();
        assert_eq!(
            historical_node
                .pg_observation(PgId::new(18))
                .unwrap()
                .observed_epoch(),
            observation_epoch
        );
    }

    #[test]
    fn restart_epoch_bump_clears_current_pg_observations_and_preserves_history() {
        let tmp = test_util::tempdir();
        let store = FileControlPlaneStore::new(tmp.path().join("control-plane.state"));
        let mut authority = SingleAuthorityControlPlane::open(store.clone()).unwrap();
        authority
            .set_node_membership(NodeId::new(1), NodeMembershipState::Active)
            .unwrap();
        assert!(heartbeat_until_serving(&mut authority, 1, 1_000).serving());
        authority
            .set_pg_acting_set(PgId::new(18), vec![NodeId::new(1)])
            .unwrap();
        let observation_epoch = authority.snapshot().cluster_epoch();
        let mut heartbeat = heartbeat_from_record(&authority, 1, observation_epoch, 2_000);
        let metadata_proof = PgMetadataProof {
            applied_log_index: 7,
            applied_log_hash: 8,
            state_digest: 9,
        };
        heartbeat.pg_observations = vec![NodePgHeartbeatObservation {
            pg_id: PgId::new(18),
            state: PgState::Peering,
            metadata_proof,
        }];
        authority.heartbeat(heartbeat, 2_000).unwrap();
        assert!(authority
            .snapshot()
            .node(NodeId::new(1))
            .unwrap()
            .pg_observation(PgId::new(18))
            .is_some());

        let restarted = SingleAuthorityControlPlane::open(store.clone()).unwrap();
        assert!(restarted.snapshot().cluster_epoch() > observation_epoch);
        assert!(restarted
            .snapshot()
            .node(NodeId::new(1))
            .unwrap()
            .pg_observation(PgId::new(18))
            .is_none());
        let historical_node = restarted
            .snapshot()
            .cluster_map_at_epoch(observation_epoch)
            .unwrap()
            .nodes()
            .iter()
            .find(|record| record.node_id() == NodeId::new(1))
            .unwrap();
        let historical_observation = historical_node.pg_observation(PgId::new(18)).unwrap();
        assert_eq!(historical_observation.observed_epoch(), observation_epoch);
        assert_eq!(historical_observation.metadata_proof(), metadata_proof);

        let persisted = store.load().unwrap().unwrap();
        assert!(persisted
            .node(NodeId::new(1))
            .unwrap()
            .pg_observation(PgId::new(18))
            .is_none());
        SingleAuthorityControlPlane::open(store).unwrap();
    }

    #[test]
    fn stale_observed_epoch_heartbeat_does_not_mutate_serving_record() {
        let tmp = test_util::tempdir();
        let store = FileControlPlaneStore::new(tmp.path().join("control-plane.state"));
        let mut authority = SingleAuthorityControlPlane::open(store).unwrap();
        authority
            .set_node_membership(NodeId::new(7), NodeMembershipState::Active)
            .unwrap();
        let serving = heartbeat_until_serving(&mut authority, 7, 100);
        assert!(serving.serving());
        assert_eq!(
            authority.deterministic_pg_primary(PgId::new(1), &[NodeId::new(7)], 100),
            Some(NodeId::new(7))
        );

        let before = authority.snapshot().node(NodeId::new(7)).unwrap().clone();
        let stale_epoch = ClusterEpoch::new(serving.cluster_epoch().get() - 1).unwrap();
        let mut stale = heartbeat(7, stale_epoch, 200);
        stale.node_incarnation = before.node_incarnation() + 1;
        stale.endpoint = "stale-node-7.sock".to_owned();
        let stale_response = authority.heartbeat(stale, 200).unwrap();
        assert!(!stale_response.serving());
        assert_eq!(stale_response.cluster_epoch(), serving.cluster_epoch());

        let after = authority.snapshot().node(NodeId::new(7)).unwrap();
        assert_eq!(after.node_incarnation(), before.node_incarnation());
        assert_eq!(after.endpoint(), before.endpoint());
        assert_eq!(after.last_observed_epoch(), before.last_observed_epoch());
        assert_eq!(after.last_heartbeat_ms(), before.last_heartbeat_ms());
        assert_eq!(after.lease_deadline_ms(), before.lease_deadline_ms());
        assert_eq!(
            authority.deterministic_pg_primary(PgId::new(1), &[NodeId::new(7)], 200),
            Some(NodeId::new(7))
        );
    }

    #[test]
    fn temporary_availability_changes_bump_epoch_without_removing_membership() {
        let tmp = test_util::tempdir();
        let store = FileControlPlaneStore::new(tmp.path().join("control-plane.state"));
        let mut authority = SingleAuthorityControlPlane::open(store).unwrap();
        authority
            .set_node_membership(NodeId::new(2), NodeMembershipState::Active)
            .unwrap();
        let healthy_epoch = authority
            .heartbeat(heartbeat(2, authority.snapshot().cluster_epoch(), 100), 100)
            .unwrap()
            .cluster_epoch();

        authority
            .mark_node_availability(NodeId::new(2), NodeAvailabilityState::Unavailable)
            .unwrap();
        let unavailable = authority.snapshot().node(NodeId::new(2)).unwrap();
        assert_eq!(unavailable.membership(), NodeMembershipState::Active);
        assert_eq!(
            unavailable.availability(),
            NodeAvailabilityState::Unavailable
        );
        assert!(authority.snapshot().cluster_epoch() > healthy_epoch);

        let unavailable_epoch = authority.snapshot().cluster_epoch();
        let recovered = authority
            .heartbeat(heartbeat(2, unavailable_epoch, 500), 500)
            .unwrap();
        assert!(recovered.cluster_epoch() > unavailable_epoch);
        assert!(!recovered.serving());
        let serving = authority
            .heartbeat(heartbeat(2, recovered.cluster_epoch(), 600), 600)
            .unwrap();
        assert!(serving.serving());
        assert_eq!(
            authority
                .snapshot()
                .node(NodeId::new(2))
                .unwrap()
                .membership(),
            NodeMembershipState::Active
        );
    }

    #[test]
    fn deterministic_primary_uses_first_healthy_serving_acting_set_member() {
        let tmp = test_util::tempdir();
        let store = FileControlPlaneStore::new(tmp.path().join("control-plane.state"));
        let mut authority = SingleAuthorityControlPlane::open(store).unwrap();
        for node_id in [1, 2, 3] {
            authority
                .set_node_membership(NodeId::new(node_id), NodeMembershipState::Active)
                .unwrap();
            assert!(heartbeat_until_serving(&mut authority, node_id, 1_000).serving());
        }
        authority
            .mark_node_availability(NodeId::new(1), NodeAvailabilityState::Unavailable)
            .unwrap();
        authority
            .set_node_membership(NodeId::new(2), NodeMembershipState::Out)
            .unwrap();
        authority
            .heartbeat(
                heartbeat_from_record(&authority, 3, authority.snapshot().cluster_epoch(), 2_000),
                2_000,
            )
            .unwrap();

        let acting_set = [NodeId::new(1), NodeId::new(2), NodeId::new(3)];
        assert_eq!(
            authority.deterministic_pg_primary(PgId::new(7), &acting_set, 2_000),
            Some(NodeId::new(3))
        );
    }

    #[test]
    fn expired_heartbeat_lease_marks_node_unavailable_and_bumps_epoch_once() {
        let tmp = test_util::tempdir();
        let store = FileControlPlaneStore::new(tmp.path().join("control-plane.state"));
        let mut authority = SingleAuthorityControlPlane::open(store.clone()).unwrap();
        authority
            .set_node_membership(NodeId::new(1), NodeMembershipState::Active)
            .unwrap();
        authority
            .set_node_membership(NodeId::new(2), NodeMembershipState::Active)
            .unwrap();
        let node_one = heartbeat_until_serving(&mut authority, 1, 1_000);
        let node_two = heartbeat_until_serving(&mut authority, 2, 1_000);
        assert!(node_one.serving());
        assert!(node_two.serving());
        let node_one_current = authority
            .heartbeat(
                heartbeat_from_record(&authority, 1, authority.snapshot().cluster_epoch(), 1_002),
                1_002,
            )
            .unwrap();
        assert!(node_one_current.serving());
        assert_eq!(
            authority.deterministic_pg_primary(
                PgId::new(1),
                &[NodeId::new(1), NodeId::new(2)],
                1_002,
            ),
            Some(NodeId::new(1))
        );
        authority
            .set_pg_acting_set(PgId::new(9), vec![NodeId::new(1), NodeId::new(2)])
            .unwrap();
        for node_id in [1, 2] {
            heartbeat_with_pg_observation(
                &mut authority,
                node_id,
                9,
                PgState::Peering,
                1_003 + u64::from(node_id),
            );
        }
        authority
            .complete_pg_peering(
                PgId::new(9),
                NodeId::new(1),
                node_incarnation(&authority, 1),
                1_050,
            )
            .unwrap();
        heartbeat_with_pg_observation(&mut authority, 1, 9, PgState::Active, 1_011);
        authority
            .heartbeat(
                heartbeat_from_record(&authority, 2, authority.snapshot().cluster_epoch(), 1_012),
                1_012,
            )
            .unwrap();
        assert_eq!(
            authority.serving_pg_primary(PgId::new(9), 1_012),
            Some(NodeId::new(1))
        );

        let before_expiry_epoch = authority.snapshot().cluster_epoch();
        let expiry = authority.expire_heartbeat_leases(1_112).unwrap();
        assert_eq!(expiry.expired_nodes(), &[NodeId::new(1), NodeId::new(2)]);
        assert_eq!(expiry.peering_pgs(), &[PgId::new(9)]);
        assert!(expiry.cluster_epoch() > before_expiry_epoch);
        assert_eq!(expiry.snapshot().cluster_epoch(), expiry.cluster_epoch());
        assert_eq!(
            expiry.snapshot().pg(PgId::new(9)).unwrap().state(),
            PgState::Peering
        );
        assert_eq!(
            authority
                .snapshot()
                .node(NodeId::new(1))
                .unwrap()
                .availability(),
            NodeAvailabilityState::Unavailable
        );
        assert_eq!(
            authority
                .snapshot()
                .node(NodeId::new(1))
                .unwrap()
                .lease_deadline_ms(),
            None
        );
        assert_eq!(
            authority.deterministic_pg_primary(
                PgId::new(1),
                &[NodeId::new(1), NodeId::new(2)],
                1_112,
            ),
            None
        );
        assert_eq!(authority.serving_pg_primary(PgId::new(9), 1_112), None);

        let repeated = authority.expire_heartbeat_leases(9_999).unwrap();
        assert_eq!(repeated.expired_nodes(), &[]);
        assert_eq!(repeated.peering_pgs(), &[]);
        assert_eq!(repeated.cluster_epoch(), expiry.cluster_epoch());

        let persisted = store.load().unwrap().unwrap();
        assert_eq!(persisted.cluster_epoch(), expiry.cluster_epoch());
        assert_eq!(
            persisted.pg(PgId::new(9)).unwrap().state(),
            PgState::Peering
        );
        assert_eq!(
            persisted.node(NodeId::new(2)).unwrap().availability(),
            NodeAvailabilityState::Unavailable
        );
    }

    #[test]
    fn heartbeat_after_expiry_must_observe_new_epoch_before_serving() {
        let tmp = test_util::tempdir();
        let store = FileControlPlaneStore::new(tmp.path().join("control-plane.state"));
        let mut authority = SingleAuthorityControlPlane::open(store).unwrap();
        authority
            .set_node_membership(NodeId::new(3), NodeMembershipState::Active)
            .unwrap();
        let serving = heartbeat_until_serving(&mut authority, 3, 1_000);
        assert!(serving.serving());

        let expiry = authority.expire_heartbeat_leases(1_101).unwrap();
        assert_eq!(expiry.expired_nodes(), &[NodeId::new(3)]);

        let stale_after_expiry = authority
            .heartbeat(
                heartbeat_from_record(&authority, 3, serving.cluster_epoch(), 1_200),
                1_200,
            )
            .unwrap();
        assert!(!stale_after_expiry.serving());
        assert_eq!(stale_after_expiry.cluster_epoch(), expiry.cluster_epoch());
        assert_eq!(
            authority
                .snapshot()
                .node(NodeId::new(3))
                .unwrap()
                .availability(),
            NodeAvailabilityState::Unavailable
        );

        let recovered = authority
            .heartbeat(
                heartbeat_from_record(&authority, 3, stale_after_expiry.cluster_epoch(), 1_300),
                1_300,
            )
            .unwrap();
        assert!(recovered.cluster_epoch() > stale_after_expiry.cluster_epoch());
        assert!(!recovered.serving());

        let caught_up = authority
            .heartbeat(
                heartbeat_from_record(&authority, 3, recovered.cluster_epoch(), 1_400),
                1_400,
            )
            .unwrap();
        assert!(caught_up.serving());
        assert_eq!(
            authority.deterministic_pg_primary(PgId::new(1), &[NodeId::new(3)], 1_400),
            Some(NodeId::new(3))
        );
    }

    #[test]
    fn recovered_earlier_primary_forces_active_pg_back_to_peering() {
        let tmp = test_util::tempdir();
        let store = FileControlPlaneStore::new(tmp.path().join("control-plane.state"));
        let mut authority = SingleAuthorityControlPlane::open(store).unwrap();
        for node_id in [1, 2] {
            authority
                .set_node_membership(NodeId::new(node_id), NodeMembershipState::Active)
                .unwrap();
            assert!(heartbeat_until_serving(&mut authority, node_id, 100).serving());
        }
        authority
            .set_pg_acting_set(PgId::new(13), vec![NodeId::new(1), NodeId::new(2)])
            .unwrap();
        heartbeat_with_pg_observation(&mut authority, 1, 13, PgState::Peering, 1_000);
        heartbeat_with_pg_observation(&mut authority, 2, 13, PgState::Peering, 1_050);
        authority
            .complete_pg_peering(
                PgId::new(13),
                NodeId::new(1),
                node_incarnation(&authority, 1),
                1_060,
            )
            .unwrap();
        heartbeat_with_pg_observation(&mut authority, 1, 13, PgState::Active, 1_070);
        authority
            .heartbeat(
                heartbeat_from_record(&authority, 2, authority.snapshot().cluster_epoch(), 1_080),
                1_080,
            )
            .unwrap();
        assert_eq!(
            authority.serving_pg_primary(PgId::new(13), 1_080),
            Some(NodeId::new(1))
        );

        let node_one_deadline = authority
            .snapshot()
            .node(NodeId::new(1))
            .unwrap()
            .lease_deadline_ms()
            .unwrap();
        let expiry = authority
            .expire_heartbeat_leases(node_one_deadline)
            .unwrap();
        assert_eq!(expiry.expired_nodes(), &[NodeId::new(1)]);
        assert_eq!(expiry.peering_pgs(), &[PgId::new(13)]);

        heartbeat_with_pg_observation(
            &mut authority,
            2,
            13,
            PgState::Peering,
            node_one_deadline + 1,
        );
        authority
            .complete_pg_peering(
                PgId::new(13),
                NodeId::new(2),
                node_incarnation(&authority, 2),
                node_one_deadline + 2,
            )
            .unwrap();
        heartbeat_with_pg_observation(
            &mut authority,
            2,
            13,
            PgState::Active,
            node_one_deadline + 3,
        );
        assert_eq!(
            authority.serving_pg_primary(PgId::new(13), node_one_deadline + 3),
            Some(NodeId::new(2))
        );

        let recovered = authority
            .heartbeat(
                heartbeat_from_record(
                    &authority,
                    1,
                    authority.snapshot().cluster_epoch(),
                    node_one_deadline + 4,
                ),
                node_one_deadline + 4,
            )
            .unwrap();
        assert!(!recovered.serving());
        assert_eq!(
            authority.snapshot().pg(PgId::new(13)).unwrap().state(),
            PgState::Peering
        );
        assert_eq!(
            authority.serving_pg_primary(PgId::new(13), node_one_deadline + 4),
            None
        );

        let caught_up = authority
            .heartbeat(
                heartbeat_from_record(
                    &authority,
                    1,
                    recovered.cluster_epoch(),
                    node_one_deadline + 5,
                ),
                node_one_deadline + 5,
            )
            .unwrap();
        assert!(caught_up.serving());
        assert_eq!(
            authority.snapshot().pg(PgId::new(13)).unwrap().state(),
            PgState::Peering
        );
        assert!(matches!(
            authority.authorize_pg_primary_service(
                PgId::new(13),
                NodeId::new(1),
                node_incarnation(&authority, 1),
                authority.snapshot().cluster_epoch(),
                node_one_deadline + 6,
            ),
            Err(ControlPlaneError::PgNotActive { pg_id: 13, .. })
        ));
    }

    #[test]
    fn stale_runtime_map_fails_closed_after_epoch_transition() {
        let tmp = test_util::tempdir();
        let store = FileControlPlaneStore::new(tmp.path().join("control-plane.state"));
        let mut authority = SingleAuthorityControlPlane::open(store).unwrap();
        authority
            .set_node_membership(NodeId::new(1), NodeMembershipState::Active)
            .unwrap();
        assert!(heartbeat_until_serving(&mut authority, 1, 1_000).serving());
        authority
            .set_pg_acting_set(PgId::new(17), vec![NodeId::new(1)])
            .unwrap();
        heartbeat_with_pg_observation(&mut authority, 1, 17, PgState::Peering, 2_000);
        authority
            .complete_pg_peering(
                PgId::new(17),
                NodeId::new(1),
                node_incarnation(&authority, 1),
                2_001,
            )
            .unwrap();
        heartbeat_with_pg_observation(&mut authority, 1, 17, PgState::Active, 2_002);

        let active_map = authority.snapshot().runtime_map(2_003).unwrap();
        let stale_frontend_cluster = crate::StorageCluster::from_runtime_map(
            NodeId::new(1),
            &active_map,
            crate::EcShape { k: 1, m: 0 },
        )
        .unwrap();
        let valid_until_ms = active_map.valid_until_ms().unwrap();
        assert_eq!(
            stale_frontend_cluster.route_map_valid_until_ms(),
            Some(valid_until_ms)
        );
        assert!(stale_frontend_cluster
            .require_route_map_valid_at(valid_until_ms - 1)
            .is_ok());

        let before_expiry_epoch = authority.snapshot().cluster_epoch();
        let expiry = authority.expire_heartbeat_leases(valid_until_ms).unwrap();
        assert_eq!(expiry.expired_nodes(), &[NodeId::new(1)]);
        assert!(expiry.cluster_epoch() > before_expiry_epoch);
        assert_eq!(
            authority.snapshot().pg(PgId::new(17)).unwrap().state(),
            PgState::Peering
        );

        assert!(matches!(
            stale_frontend_cluster.require_route_map_valid_at(valid_until_ms),
            Err(crate::StoreError::RouteMapExpired {
                cluster_epoch,
                valid_until_ms: expired_at,
                now_ms,
            }) if cluster_epoch == active_map.cluster_epoch()
                && expired_at == valid_until_ms
                && now_ms == valid_until_ms
        ));
        assert_eq!(
            authority
                .snapshot()
                .runtime_map(valid_until_ms)
                .unwrap()
                .pg_routes()[0]
                .state(),
            PgState::Peering
        );
    }

    #[test]
    fn stale_primary_authorization_cannot_validate_after_epoch_transition() {
        let tmp = test_util::tempdir();
        let store = FileControlPlaneStore::new(tmp.path().join("control-plane.state"));
        let mut authority = SingleAuthorityControlPlane::open(store).unwrap();
        authority
            .set_node_membership(NodeId::new(1), NodeMembershipState::Active)
            .unwrap();
        assert!(heartbeat_until_serving(&mut authority, 1, 1_000).serving());
        authority
            .set_pg_acting_set(PgId::new(18), vec![NodeId::new(1)])
            .unwrap();
        heartbeat_with_pg_observation(&mut authority, 1, 18, PgState::Peering, 2_000);
        authority
            .complete_pg_peering(
                PgId::new(18),
                NodeId::new(1),
                node_incarnation(&authority, 1),
                2_001,
            )
            .unwrap();
        let active = heartbeat_with_pg_observation(&mut authority, 1, 18, PgState::Active, 2_002);
        let authorization = authority
            .authorize_pg_operation(
                PgServiceOperation::MetadataWrite,
                PgId::new(18),
                NodeId::new(1),
                node_incarnation(&authority, 1),
                active.cluster_epoch(),
                2_003,
            )
            .unwrap();
        assert!(authority
            .validate_pg_operation_authorization(&authorization, 2_004)
            .is_ok());

        let lease_deadline_ms = authorization.primary().lease_deadline_ms();
        let expiry = authority
            .expire_heartbeat_leases(lease_deadline_ms)
            .unwrap();
        assert_eq!(expiry.expired_nodes(), &[NodeId::new(1)]);
        assert_eq!(
            authority.snapshot().pg(PgId::new(18)).unwrap().state(),
            PgState::Peering
        );

        assert!(matches!(
            authority.validate_pg_operation_authorization(&authorization, lease_deadline_ms),
            Err(ControlPlaneError::StaleAuthorizationEpoch {
                cluster_epoch,
                current_epoch,
            }) if cluster_epoch == active.cluster_epoch()
                && current_epoch == expiry.cluster_epoch()
        ));
        assert!(matches!(
            authority.authorize_pg_primary_service(
                PgId::new(18),
                NodeId::new(1),
                node_incarnation(&authority, 1),
                expiry.cluster_epoch(),
                lease_deadline_ms,
            ),
            Err(ControlPlaneError::NodeLeaseExpired { node_id: 1, .. })
        ));
    }

    #[test]
    fn storage_node_refresh_after_epoch_transition_cannot_keep_stale_active_route() {
        let tmp = test_util::tempdir();
        let store = FileControlPlaneStore::new(tmp.path().join("control-plane.state"));
        let mut authority = SingleAuthorityControlPlane::open(store).unwrap();
        authority
            .set_node_membership(NodeId::new(1), NodeMembershipState::Active)
            .unwrap();
        assert!(heartbeat_until_serving(&mut authority, 1, 1_000).serving());
        authority
            .set_pg_acting_set(PgId::new(19), vec![NodeId::new(1)])
            .unwrap();
        heartbeat_with_pg_observation(&mut authority, 1, 19, PgState::Peering, 2_000);
        authority
            .complete_pg_peering(
                PgId::new(19),
                NodeId::new(1),
                node_incarnation(&authority, 1),
                2_001,
            )
            .unwrap();
        let active = heartbeat_with_pg_observation(&mut authority, 1, 19, PgState::Active, 2_002);
        let active_epoch = active.cluster_epoch();
        let active_map = authority.snapshot().runtime_map(2_003).unwrap();
        let active_valid_until_ms = active_map.valid_until_ms().unwrap();
        assert_eq!(active_map.pg_routes()[0].state(), PgState::Active);

        let expiry = authority
            .expire_heartbeat_leases(active_valid_until_ms)
            .unwrap();
        assert_eq!(expiry.expired_nodes(), &[NodeId::new(1)]);
        assert!(expiry.cluster_epoch() > active_epoch);
        assert_eq!(
            authority.snapshot().pg(PgId::new(19)).unwrap().state(),
            PgState::Peering
        );

        let mut stale_active_heartbeat =
            heartbeat_from_record(&authority, 1, active_epoch, active_valid_until_ms + 1);
        stale_active_heartbeat.pg_observations = vec![NodePgHeartbeatObservation {
            pg_id: PgId::new(19),
            state: PgState::Active,
            metadata_proof: PgMetadataProof::empty(),
        }];
        let refresh = authority
            .refresh_node_heartbeat(stale_active_heartbeat, active_valid_until_ms + 1)
            .unwrap();

        assert!(!refresh.lease().serving());
        assert_eq!(refresh.lease().cluster_epoch(), expiry.cluster_epoch());
        assert_eq!(
            refresh.runtime_map().cluster_epoch(),
            expiry.cluster_epoch()
        );
        assert_eq!(
            refresh.runtime_map().pg_routes()[0].state(),
            PgState::Peering
        );
        assert_eq!(
            refresh.runtime_map().pg_routes()[0].primary_lease_deadline_ms(),
            None
        );
        let record = authority.snapshot().node(NodeId::new(1)).unwrap();
        assert_eq!(record.availability(), NodeAvailabilityState::Unavailable);
        assert_eq!(record.last_observed_epoch(), Some(active_epoch));
    }

    #[test]
    fn acting_set_change_fences_old_primary_token_until_new_peering_completes() {
        let tmp = test_util::tempdir();
        let store = FileControlPlaneStore::new(tmp.path().join("control-plane.state"));
        let mut authority = SingleAuthorityControlPlane::open(store).unwrap();
        for node_id in [1, 2] {
            authority
                .set_node_membership(NodeId::new(node_id), NodeMembershipState::Active)
                .unwrap();
            assert!(heartbeat_until_serving(&mut authority, node_id, 1_000).serving());
        }
        authority
            .set_pg_acting_set(PgId::new(20), vec![NodeId::new(1)])
            .unwrap();
        heartbeat_with_pg_observation(&mut authority, 1, 20, PgState::Peering, 2_000);
        authority
            .complete_pg_peering(
                PgId::new(20),
                NodeId::new(1),
                node_incarnation(&authority, 1),
                2_001,
            )
            .unwrap();
        let active = heartbeat_with_pg_observation(&mut authority, 1, 20, PgState::Active, 2_002);
        let old_epoch = active.cluster_epoch();
        let old_authorization = authority
            .authorize_pg_operation(
                PgServiceOperation::MetadataWrite,
                PgId::new(20),
                NodeId::new(1),
                node_incarnation(&authority, 1),
                old_epoch,
                2_003,
            )
            .unwrap();
        authority
            .validate_pg_operation_authorization(&old_authorization, 2_004)
            .unwrap();

        authority
            .set_pg_acting_set(PgId::new(20), vec![NodeId::new(2)])
            .unwrap();
        let peering_epoch = authority.snapshot().cluster_epoch();
        assert!(peering_epoch > old_epoch);
        assert_eq!(
            authority.snapshot().pg(PgId::new(20)).unwrap().state(),
            PgState::Peering
        );
        assert!(matches!(
            authority.validate_pg_operation_authorization(&old_authorization, 2_005),
            Err(ControlPlaneError::StaleAuthorizationEpoch {
                cluster_epoch,
                current_epoch,
            }) if cluster_epoch == old_epoch && current_epoch == peering_epoch
        ));
        for node_id in [1, 2] {
            let now_ms = 2_006 + u64::from(node_id);
            authority
                .heartbeat(
                    heartbeat_from_record(&authority, node_id, peering_epoch, now_ms),
                    now_ms,
                )
                .unwrap();
        }
        assert!(matches!(
            authority.authorize_pg_operation(
                PgServiceOperation::MetadataWrite,
                PgId::new(20),
                NodeId::new(1),
                node_incarnation(&authority, 1),
                peering_epoch,
                2_009,
            ),
            Err(ControlPlaneError::PgNotActive {
                pg_id: 20,
                state: PgState::Peering,
                ..
            })
        ));
        assert!(matches!(
            authority.authorize_pg_operation(
                PgServiceOperation::MetadataWrite,
                PgId::new(20),
                NodeId::new(2),
                node_incarnation(&authority, 2),
                peering_epoch,
                2_010,
            ),
            Err(ControlPlaneError::PgNotActive {
                pg_id: 20,
                state: PgState::Peering,
                ..
            })
        ));

        let node_two_proof = PgMetadataProof {
            applied_log_index: 77,
            applied_log_hash: 0xabcddcba,
            state_digest: 0x12344321,
        };
        let mut node_two_peering = heartbeat_from_record(&authority, 2, peering_epoch, 2_011);
        node_two_peering.pg_observations = vec![NodePgHeartbeatObservation {
            pg_id: PgId::new(20),
            state: PgState::Peering,
            metadata_proof: node_two_proof,
        }];
        authority.heartbeat(node_two_peering, 2_011).unwrap();
        authority
            .complete_pg_peering(
                PgId::new(20),
                NodeId::new(2),
                node_incarnation(&authority, 2),
                2_012,
            )
            .unwrap();
        let mut new_active_heartbeat =
            heartbeat_from_record(&authority, 2, authority.snapshot().cluster_epoch(), 2_013);
        new_active_heartbeat.pg_observations = vec![NodePgHeartbeatObservation {
            pg_id: PgId::new(20),
            state: PgState::Active,
            metadata_proof: node_two_proof,
        }];
        let new_active = authority.heartbeat(new_active_heartbeat, 2_013).unwrap();
        let new_epoch = new_active.cluster_epoch();
        assert!(new_epoch > peering_epoch);

        authority
            .heartbeat(
                heartbeat_from_record(&authority, 1, new_epoch, 2_014),
                2_014,
            )
            .unwrap();
        assert!(matches!(
            authority.authorize_pg_operation(
                PgServiceOperation::MetadataWrite,
                PgId::new(20),
                NodeId::new(1),
                node_incarnation(&authority, 1),
                new_epoch,
                2_015,
            ),
            Err(ControlPlaneError::NodeNotPgPrimary {
                pg_id: 20,
                node_id: 1,
                primary_node_id: 2,
                ..
            })
        ));

        let new_authorization = authority
            .authorize_pg_operation(
                PgServiceOperation::MetadataWrite,
                PgId::new(20),
                NodeId::new(2),
                node_incarnation(&authority, 2),
                new_epoch,
                2_016,
            )
            .unwrap();
        assert_eq!(new_authorization.primary_node_id(), NodeId::new(2));
        authority
            .validate_pg_operation_authorization(&new_authorization, 2_017)
            .unwrap();
    }

    #[test]
    fn failed_expiry_persist_does_not_expose_uncommitted_epoch_or_map() {
        let tmp = test_util::tempdir();
        let store = FileControlPlaneStore::new(tmp.path().join("control-plane.state"));
        let mut authority = SingleAuthorityControlPlane::open(store).unwrap();
        authority
            .set_node_membership(NodeId::new(12), NodeMembershipState::Active)
            .unwrap();
        assert!(heartbeat_until_serving(&mut authority, 12, 1_000).serving());
        let committed = authority.snapshot().clone();
        assert_eq!(
            committed.node(NodeId::new(12)).unwrap().availability(),
            NodeAvailabilityState::Healthy
        );

        let failing_store = FailingStore::new(committed.clone());
        let mut restarted = SingleAuthorityControlPlane::open(failing_store).unwrap();
        assert!(restarted
            .heartbeat(
                heartbeat_from_record(&restarted, 12, restarted.snapshot().cluster_epoch(), 1_001,),
                1_001
            )
            .unwrap()
            .serving());
        let visible_before_failure = restarted.snapshot().clone();
        restarted.store.fail_saves();
        assert!(matches!(
            restarted.expire_heartbeat_leases(1_101),
            Err(ControlPlaneError::Io {
                context: "test save failure",
                ..
            })
        ));
        assert_eq!(restarted.snapshot(), &visible_before_failure);
        assert_eq!(
            restarted.deterministic_pg_primary(PgId::new(1), &[NodeId::new(12)], 1_001),
            Some(NodeId::new(12))
        );
    }

    #[test]
    fn heartbeat_rejects_unknown_removed_and_zero_duration_nodes() {
        let tmp = test_util::tempdir();
        let store = FileControlPlaneStore::new(tmp.path().join("control-plane.state"));
        let mut authority = SingleAuthorityControlPlane::open(store).unwrap();
        assert!(matches!(
            authority.heartbeat(heartbeat(9, ClusterEpoch::INITIAL, 1), 1),
            Err(ControlPlaneError::UnknownNode { node_id: 9 })
        ));

        authority
            .set_node_membership(NodeId::new(9), NodeMembershipState::Removed)
            .unwrap();
        assert!(matches!(
            authority.heartbeat(heartbeat(9, authority.snapshot().cluster_epoch(), 2), 2),
            Err(ControlPlaneError::NodeCannotReceiveLease { node_id: 9, .. })
        ));

        authority
            .set_node_membership(NodeId::new(10), NodeMembershipState::Active)
            .unwrap();
        let mut invalid = heartbeat(10, authority.snapshot().cluster_epoch(), 3);
        invalid.requested_lease_duration_ms = 0;
        assert!(matches!(
            authority.heartbeat(invalid, 3),
            Err(ControlPlaneError::InvalidLeaseDuration)
        ));
    }

    #[test]
    fn heartbeat_rejects_overlong_lease_duration() {
        let tmp = test_util::tempdir();
        let store = FileControlPlaneStore::new(tmp.path().join("control-plane.state"));
        let mut authority = SingleAuthorityControlPlane::open(store).unwrap();
        authority
            .set_node_membership(NodeId::new(10), NodeMembershipState::Active)
            .unwrap();

        let mut invalid = heartbeat(10, authority.snapshot().cluster_epoch(), 3);
        invalid.requested_lease_duration_ms = MAX_HEARTBEAT_LEASE_MS + 1;
        assert!(matches!(
            authority.heartbeat(invalid, 3),
            Err(ControlPlaneError::LeaseDurationTooLong {
                requested_ms,
                max_ms,
            }) if requested_ms == MAX_HEARTBEAT_LEASE_MS + 1
                && max_ms == MAX_HEARTBEAT_LEASE_MS
        ));
    }

    #[test]
    fn primary_selection_requires_unexpired_authority_lease() {
        let tmp = test_util::tempdir();
        let store = FileControlPlaneStore::new(tmp.path().join("control-plane.state"));
        let mut authority = SingleAuthorityControlPlane::open(store).unwrap();
        authority
            .set_node_membership(NodeId::new(42), NodeMembershipState::Active)
            .unwrap();
        let serving = heartbeat_until_serving(&mut authority, 42, 1_000);
        assert!(serving.serving());
        assert_eq!(
            authority.deterministic_pg_primary(
                PgId::new(1),
                &[NodeId::new(42)],
                serving.lease_deadline_ms() - 1,
            ),
            Some(NodeId::new(42))
        );
        assert_eq!(
            authority.deterministic_pg_primary(
                PgId::new(1),
                &[NodeId::new(42)],
                serving.lease_deadline_ms(),
            ),
            None
        );

        authority
            .set_pg_acting_set(PgId::new(21), vec![NodeId::new(42)])
            .unwrap();
        heartbeat_with_pg_observation(&mut authority, 42, 21, PgState::Peering, 1_010);
        authority
            .complete_pg_peering(
                PgId::new(21),
                NodeId::new(42),
                node_incarnation(&authority, 42),
                1_020,
            )
            .unwrap();
        let active = heartbeat_with_pg_observation(&mut authority, 42, 21, PgState::Active, 1_030);
        assert_eq!(
            authority.serving_pg_primary(PgId::new(21), active.lease_deadline_ms() - 1),
            Some(NodeId::new(42))
        );
        assert_eq!(
            authority.serving_pg_primary(PgId::new(21), active.lease_deadline_ms()),
            None
        );
    }

    #[test]
    fn removed_nodes_cannot_rejoin_or_be_marked_healthy() {
        let tmp = test_util::tempdir();
        let store = FileControlPlaneStore::new(tmp.path().join("control-plane.state"));
        let mut authority = SingleAuthorityControlPlane::open(store).unwrap();
        authority
            .set_node_membership(NodeId::new(11), NodeMembershipState::Active)
            .unwrap();
        authority
            .set_node_membership(NodeId::new(11), NodeMembershipState::Removed)
            .unwrap();

        assert!(matches!(
            authority.set_node_membership(NodeId::new(11), NodeMembershipState::Active),
            Err(ControlPlaneError::RemovedNodeCannotRejoin { node_id: 11 })
        ));
        assert!(matches!(
            authority.mark_node_availability(NodeId::new(11), NodeAvailabilityState::Healthy),
            Err(ControlPlaneError::NodeCannotReceiveLease { node_id: 11, .. })
        ));
    }
}
