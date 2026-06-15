use std::collections::{BTreeMap, BTreeSet};
use std::io::Write as _;
use std::num::NonZeroU64;
use std::path::{Path, PathBuf};

use placement::NodeId;
use thiserror::Error;

use crate::{ClusterEpoch, PgId, PgState};

const CLUSTER_MAP_HISTORY_LIMIT: usize = 32;

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

    fn can_serve_primary(&self, cluster_epoch: ClusterEpoch) -> bool {
        self.membership.can_serve_primary()
            && self.availability == NodeAvailabilityState::Healthy
            && self.last_observed_epoch == Some(cluster_epoch)
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

    fn bump_authority_after_restart(&mut self) -> Result<(), ControlPlaneError> {
        self.authority_incarnation = self.authority_incarnation.next()?;
        self.cluster_epoch = next_epoch(self.cluster_epoch)?;
        Ok(())
    }

    fn bump_epoch(&mut self) -> Result<(), ControlPlaneError> {
        self.cluster_epoch = next_epoch(self.cluster_epoch)?;
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
}

impl PgControlRecord {
    fn new(pg_id: PgId, acting_set: Vec<NodeId>) -> Self {
        Self {
            pg_id,
            state: PgState::Peering,
            acting_set,
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
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NodeHeartbeat {
    pub node_id: NodeId,
    pub node_incarnation: u64,
    pub endpoint: String,
    pub observed_epoch: ClusterEpoch,
    pub now_ms: u64,
    pub lease_duration_ms: u64,
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

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct NodeServiceAuthorization {
    authority_incarnation: AuthorityIncarnation,
    cluster_epoch: ClusterEpoch,
    node_id: NodeId,
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
    pub fn lease_deadline_ms(&self) -> u64 {
        self.lease_deadline_ms
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
        if self.deterministic_pg_primary(pg_id, record.acting_set()) != Some(primary) {
            return Err(ControlPlaneError::PgPrimaryNotServingCurrentEpoch {
                pg_id: pg_id.get(),
                node_id: primary.as_u32(),
            });
        }

        let mut next_snapshot = self.snapshot.clone();
        let record = next_snapshot
            .pgs
            .get_mut(&pg_id)
            .expect("PG record validated before peering completion");
        if record.state != PgState::Active {
            record.state = PgState::Active;
            next_snapshot.bump_epoch()?;
            self.commit_snapshot(next_snapshot)?;
        }
        Ok(self.snapshot.clone())
    }

    pub fn heartbeat(
        &mut self,
        heartbeat: NodeHeartbeat,
    ) -> Result<HeartbeatLease, ControlPlaneError> {
        if heartbeat.lease_duration_ms == 0 {
            return Err(ControlPlaneError::InvalidLeaseDuration);
        }
        let lease_deadline_ms = heartbeat
            .now_ms
            .checked_add(heartbeat.lease_duration_ms)
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
                lease_deadline_ms: record.lease_deadline_ms.unwrap_or(heartbeat.now_ms),
                serving: false,
                snapshot: self.snapshot.clone(),
            });
        }

        let mut epoch_changed = false;
        let mut next_snapshot = self.snapshot.clone();
        {
            let record = next_snapshot
                .nodes
                .get_mut(&heartbeat.node_id)
                .expect("node record validated before heartbeat mutation");
            if heartbeat.node_incarnation > record.node_incarnation {
                record.node_incarnation = heartbeat.node_incarnation;
                epoch_changed = true;
            }
            if record.endpoint != heartbeat.endpoint {
                record.endpoint = heartbeat.endpoint;
                epoch_changed = true;
            }
            if record.availability != NodeAvailabilityState::Healthy {
                record.availability = NodeAvailabilityState::Healthy;
                epoch_changed = true;
            }
            record.last_observed_epoch = Some(heartbeat.observed_epoch);
            record.last_heartbeat_ms = Some(heartbeat.now_ms);
            record.lease_deadline_ms = Some(lease_deadline_ms);
        }
        if epoch_changed {
            next_snapshot.bump_epoch()?;
        }
        self.commit_snapshot(next_snapshot)?;
        let serving = self
            .snapshot
            .node(heartbeat.node_id)
            .is_some_and(|record| record.can_serve_primary(self.snapshot.cluster_epoch));
        Ok(HeartbeatLease {
            authority_incarnation: self.snapshot.authority_incarnation,
            cluster_epoch: self.snapshot.cluster_epoch,
            node_id: heartbeat.node_id,
            lease_deadline_ms,
            serving,
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
        if !record.can_serve_primary(self.snapshot.cluster_epoch) {
            return Err(ControlPlaneError::NodeNotServingCurrentEpoch {
                node_id: node_id.as_u32(),
                cluster_epoch: self.snapshot.cluster_epoch,
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
        Ok(NodeServiceAuthorization {
            authority_incarnation: self.snapshot.authority_incarnation,
            cluster_epoch: self.snapshot.cluster_epoch,
            node_id,
            lease_deadline_ms,
        })
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
        let serving_primary = self
            .deterministic_pg_primary(pg_id, record.acting_set())
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
        Ok(PgPrimaryAuthorization {
            authority_incarnation: self.snapshot.authority_incarnation,
            cluster_epoch: self.snapshot.cluster_epoch,
            pg_id,
            primary_node_id,
            lease_deadline_ms: node_authorization.lease_deadline_ms(),
        })
    }

    #[must_use]
    pub fn serving_pg_primary(&self, pg_id: PgId) -> Option<NodeId> {
        let record = self.snapshot.pgs.get(&pg_id)?;
        if record.state != PgState::Active {
            return None;
        }
        self.deterministic_pg_primary(pg_id, record.acting_set())
    }

    #[must_use]
    pub fn deterministic_pg_primary(&self, _pg_id: PgId, acting_set: &[NodeId]) -> Option<NodeId> {
        acting_set.iter().copied().find(|node_id| {
            self.snapshot
                .nodes
                .get(node_id)
                .is_some_and(|record| record.can_serve_primary(self.snapshot.cluster_epoch))
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

    #[error("PG {pg_id} Active state requires complete_pg_peering")]
    ActivePgRequiresPeeringComplete { pg_id: u32 },

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

    #[error("node {node_id} is not serving cluster epoch {cluster_epoch}")]
    NodeNotServingCurrentEpoch {
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
    out.push_str("version=3\n");
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
    format!(
        "{},{},{}",
        record.pg_id.get(),
        pg_state_as_str(record.state),
        format_node_list(&record.acting_set)
    )
}

fn parse_snapshot(contents: &str) -> Result<ClusterControlSnapshot, ControlPlaneError> {
    let mut version = None;
    let mut authority_incarnation = None;
    let mut cluster_epoch = None;
    let mut nodes = BTreeMap::new();
    let mut pgs = BTreeMap::new();
    let mut pg_lines = BTreeMap::new();
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
            let (epoch, record) = parse_history_node_record(line_number, value)?;
            let history_record = history
                .get_mut(&epoch)
                .ok_or_else(|| parse_error(line_number, "history node references unknown epoch"))?;
            if history_record.node_ids.insert(record.node_id) {
                history_record.record.nodes.push(record);
            } else {
                return Err(parse_error(line_number, "duplicate history node record"));
            }
        } else if let Some(value) = line.strip_prefix("history_pg=") {
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
        } else if let Some(value) = line.strip_prefix("pg=") {
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
    if !matches!(version, 2 | 3) {
        return Err(parse_error(
            0,
            "missing or unsupported control-plane state version",
        ));
    }
    let cluster_epoch = cluster_epoch.ok_or_else(|| parse_error(0, "missing cluster epoch"))?;
    if version == 2 && !history.is_empty() {
        return Err(parse_error(
            0,
            "history records require control-plane state version 3",
        ));
    }
    validate_current_pgs(&pgs, &pg_lines, &nodes)?;
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
}

impl ParsedHistoryRecord {
    fn new(record: ClusterMapHistoryRecord, line: usize) -> Self {
        Self {
            record,
            line,
            node_ids: BTreeSet::new(),
            pg_ids: BTreeSet::new(),
            pg_lines: BTreeMap::new(),
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
    })
}

fn parse_pg_record(line: usize, value: &str) -> Result<PgControlRecord, ControlPlaneError> {
    let fields: Vec<&str> = value.split(',').collect();
    if fields.len() != 3 {
        return Err(parse_error(line, "PG record must have three fields"));
    }
    let pg_id = PgId::new(parse_u32(line, fields[0], "PG id")?);
    let state = pg_state_from_str(fields[1])?;
    let acting_set = parse_node_list(line, fields[2])?;
    if acting_set.is_empty() {
        return Err(parse_error(line, "PG acting set must not be empty"));
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
    })
}

fn option_u64(value: Option<u64>) -> String {
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

    fn heartbeat(node_id: u32, observed_epoch: ClusterEpoch, now_ms: u64) -> NodeHeartbeat {
        NodeHeartbeat {
            node_id: NodeId::new(node_id),
            node_incarnation: 10 + u64::from(node_id),
            endpoint: format!("node-{node_id}.sock"),
            observed_epoch,
            now_ms,
            lease_duration_ms: 100,
        }
    }

    fn heartbeat_until_serving(
        authority: &mut SingleAuthorityControlPlane<FileControlPlaneStore>,
        node_id: u32,
        now_ms: u64,
    ) -> HeartbeatLease {
        let first = authority
            .heartbeat(heartbeat(
                node_id,
                authority.snapshot().cluster_epoch(),
                now_ms,
            ))
            .unwrap();
        if first.serving() {
            first
        } else {
            authority
                .heartbeat(heartbeat_from_record(
                    authority,
                    node_id,
                    first.cluster_epoch(),
                    now_ms + 1,
                ))
                .unwrap()
        }
    }

    fn heartbeat_from_record<S: ControlPlaneStore>(
        authority: &SingleAuthorityControlPlane<S>,
        node_id: u32,
        observed_epoch: ClusterEpoch,
        now_ms: u64,
    ) -> NodeHeartbeat {
        let record = authority.snapshot().node(NodeId::new(node_id)).unwrap();
        let mut heartbeat = heartbeat(node_id, observed_epoch, now_ms);
        heartbeat.node_incarnation = record.node_incarnation();
        heartbeat.endpoint = record.endpoint().to_owned();
        heartbeat
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
            .heartbeat(heartbeat(1, authority.snapshot().cluster_epoch(), 1_000))
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
    fn file_backed_authority_loads_version_two_state_without_history() {
        let tmp = test_util::tempdir();
        let path = tmp.path().join("control-plane.state");
        std::fs::write(
            &path,
            "version=2\nauthority_incarnation=1\ncluster_epoch=1\nnode=1,active,healthy,11,1,100,200,6e6f64652d312e736f636b\n",
        )
        .unwrap();
        let store = FileControlPlaneStore::new(path);
        let authority = SingleAuthorityControlPlane::open(store).unwrap();
        assert_eq!(authority.snapshot().cluster_map_history().len(), 1);
        let loaded_epoch = ClusterEpoch::INITIAL;
        let history = authority
            .snapshot()
            .cluster_map_at_epoch(loaded_epoch)
            .unwrap();
        assert_eq!(history.nodes().len(), 1);
        assert_eq!(history.nodes()[0].endpoint(), "node-1.sock");
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
            "version=2\nauthority_incarnation=1\ncluster_epoch=1\npg=7,peering,1:1\n",
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
                "version=3\n",
                "authority_incarnation=1\n",
                "cluster_epoch=2\n",
                "node=1,active,healthy,11,2,100,200,6e6f64652d312e736f636b\n",
                "pg=7,active,1:99\n",
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
    fn file_backed_authority_rejects_history_in_version_two_state() {
        let tmp = test_util::tempdir();
        let path = tmp.path().join("control-plane.state");
        std::fs::write(
            &path,
            "version=2\nauthority_incarnation=1\ncluster_epoch=2\nhistory=1,1\n",
        )
        .unwrap();
        let store = FileControlPlaneStore::new(path);
        assert!(matches!(
            SingleAuthorityControlPlane::open(store),
            Err(ControlPlaneError::Parse { message, .. })
                if message == "history records require control-plane state version 3"
        ));
    }

    #[test]
    fn file_backed_authority_rejects_current_or_future_history_epochs() {
        let tmp = test_util::tempdir();
        let path = tmp.path().join("control-plane.state");
        std::fs::write(
            &path,
            "version=3\nauthority_incarnation=1\ncluster_epoch=2\nhistory=2,1\n",
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
                "version=3\n",
                "authority_incarnation=1\n",
                "cluster_epoch=3\n",
                "history=2,1\n",
                "history_node=2,1,active,healthy,11,2,100,200,6e6f64652d312e736f636b\n",
                "history_pg=2,7,peering,1:2\n",
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
            .heartbeat(heartbeat(3, membership_epoch, 2_000))
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
            .heartbeat(heartbeat(4, authority.snapshot().cluster_epoch(), 100))
            .unwrap();

        let mut stale = heartbeat(4, first.cluster_epoch(), 200);
        stale.node_incarnation -= 1;
        assert!(matches!(
            authority.heartbeat(stale),
            Err(ControlPlaneError::StaleNodeIncarnation { node_id: 4, .. })
        ));

        let mut restarted_node = heartbeat(4, first.cluster_epoch(), 300);
        restarted_node.node_incarnation += 1;
        let fenced = authority.heartbeat(restarted_node).unwrap();
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
            .heartbeat(heartbeat_from_record(
                &authority,
                4,
                fenced.cluster_epoch(),
                400,
            ))
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
        let changed = authority.heartbeat(moved).unwrap();
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
            authority.deterministic_pg_primary(PgId::new(1), &[NodeId::new(5)]),
            None
        );

        let caught_up = authority
            .heartbeat(heartbeat_from_record(
                &authority,
                5,
                changed.cluster_epoch(),
                300,
            ))
            .unwrap();
        assert!(caught_up.serving());
        assert_eq!(
            authority.deterministic_pg_primary(PgId::new(1), &[NodeId::new(5)]),
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
        assert_eq!(authority.serving_pg_primary(PgId::new(7)), None);

        let persisted = store.load().unwrap().unwrap();
        let persisted_pg = persisted.pg(PgId::new(7)).unwrap();
        assert_eq!(persisted_pg.state(), PgState::Peering);
        assert_eq!(persisted_pg.acting_set(), &[NodeId::new(1), NodeId::new(2)]);
        assert!(std::fs::metadata(store_path).unwrap().is_file());
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
            .heartbeat(heartbeat_from_record(
                &authority,
                1,
                authority.snapshot().cluster_epoch(),
                1_002,
            ))
            .unwrap();
        authority
            .set_pg_acting_set(PgId::new(8), vec![NodeId::new(1), NodeId::new(2)])
            .unwrap();
        assert_eq!(authority.serving_pg_primary(PgId::new(8)), None);

        assert!(matches!(
            authority.set_pg_state(PgId::new(8), PgState::Active),
            Err(ControlPlaneError::ActivePgRequiresPeeringComplete { pg_id: 8 })
        ));
        assert_eq!(authority.serving_pg_primary(PgId::new(8)), None);
        for node_id in [1, 2] {
            authority
                .heartbeat(heartbeat_from_record(
                    &authority,
                    node_id,
                    authority.snapshot().cluster_epoch(),
                    2_000 + u64::from(node_id),
                ))
                .unwrap();
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
        assert_eq!(authority.serving_pg_primary(PgId::new(8)), None);
        for node_id in [1, 2] {
            authority
                .heartbeat(heartbeat_from_record(
                    &authority,
                    node_id,
                    authority.snapshot().cluster_epoch(),
                    3_000 + u64::from(node_id),
                ))
                .unwrap();
        }
        assert_eq!(
            authority.serving_pg_primary(PgId::new(8)),
            Some(NodeId::new(1))
        );
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
        authority
            .heartbeat(heartbeat_from_record(
                &authority,
                1,
                authority.snapshot().cluster_epoch(),
                1_050,
            ))
            .unwrap();
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

        authority
            .heartbeat(heartbeat_from_record(
                &authority,
                1,
                authority.snapshot().cluster_epoch(),
                lease_deadline + 1,
            ))
            .unwrap();
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

        let authorized = authority
            .authorize_node_service(
                NodeId::new(1),
                record.node_incarnation(),
                serving.cluster_epoch(),
                150,
            )
            .unwrap();
        assert_eq!(authorized.node_id(), NodeId::new(1));
        assert_eq!(authorized.cluster_epoch(), serving.cluster_epoch());
        assert_eq!(
            authorized.authority_incarnation(),
            authority.snapshot().authority_incarnation()
        );
        assert_eq!(
            authorized.lease_deadline_ms(),
            record.lease_deadline_ms().unwrap()
        );

        assert!(matches!(
            authority.authorize_node_service(
                NodeId::new(1),
                record.node_incarnation() + 1,
                serving.cluster_epoch(),
                150,
            ),
            Err(ControlPlaneError::NodeIncarnationMismatch { node_id: 1, .. })
        ));
        assert!(matches!(
            authority.authorize_node_service(
                NodeId::new(1),
                record.node_incarnation(),
                ClusterEpoch::INITIAL,
                150,
            ),
            Err(ControlPlaneError::StaleNodeObservedEpoch { node_id: 1, .. })
        ));
        assert!(matches!(
            authority.authorize_node_service(
                NodeId::new(1),
                record.node_incarnation(),
                serving.cluster_epoch(),
                record.lease_deadline_ms().unwrap(),
            ),
            Err(ControlPlaneError::NodeLeaseExpired { node_id: 1, .. })
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
            .heartbeat(heartbeat_from_record(
                &authority,
                1,
                authority.snapshot().cluster_epoch(),
                1_002,
            ))
            .unwrap();
        authority
            .set_pg_acting_set(PgId::new(10), vec![NodeId::new(1), NodeId::new(2)])
            .unwrap();
        for node_id in [1, 2] {
            authority
                .heartbeat(heartbeat_from_record(
                    &authority,
                    node_id,
                    authority.snapshot().cluster_epoch(),
                    2_000 + u64::from(node_id),
                ))
                .unwrap();
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
        for node_id in [1, 2] {
            authority
                .heartbeat(heartbeat_from_record(
                    &authority,
                    node_id,
                    authority.snapshot().cluster_epoch(),
                    3_000 + u64::from(node_id),
                ))
                .unwrap();
        }
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
    fn stale_observed_epoch_heartbeat_returns_map_without_becoming_serving() {
        let tmp = test_util::tempdir();
        let store = FileControlPlaneStore::new(tmp.path().join("control-plane.state"));
        let mut authority = SingleAuthorityControlPlane::open(store).unwrap();
        authority
            .set_node_membership(NodeId::new(6), NodeMembershipState::Active)
            .unwrap();
        let stale_epoch = ClusterEpoch::INITIAL;
        let lease = authority.heartbeat(heartbeat(6, stale_epoch, 100)).unwrap();
        assert!(!lease.serving());
        assert_eq!(lease.snapshot().cluster_epoch(), lease.cluster_epoch());
        assert_eq!(
            authority.deterministic_pg_primary(PgId::new(1), &[NodeId::new(6)]),
            None
        );

        let caught_up = authority
            .heartbeat(heartbeat(6, lease.cluster_epoch(), 200))
            .unwrap();
        assert!(!caught_up.serving());
        let final_lease = authority
            .heartbeat(heartbeat(6, caught_up.cluster_epoch(), 300))
            .unwrap();
        assert!(final_lease.serving());
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
            authority.deterministic_pg_primary(PgId::new(1), &[NodeId::new(7)]),
            Some(NodeId::new(7))
        );

        let before = authority.snapshot().node(NodeId::new(7)).unwrap().clone();
        let stale_epoch = ClusterEpoch::new(serving.cluster_epoch().get() - 1).unwrap();
        let mut stale = heartbeat(7, stale_epoch, 200);
        stale.node_incarnation = before.node_incarnation() + 1;
        stale.endpoint = "stale-node-7.sock".to_owned();
        let stale_response = authority.heartbeat(stale).unwrap();
        assert!(!stale_response.serving());
        assert_eq!(stale_response.cluster_epoch(), serving.cluster_epoch());

        let after = authority.snapshot().node(NodeId::new(7)).unwrap();
        assert_eq!(after.node_incarnation(), before.node_incarnation());
        assert_eq!(after.endpoint(), before.endpoint());
        assert_eq!(after.last_observed_epoch(), before.last_observed_epoch());
        assert_eq!(after.last_heartbeat_ms(), before.last_heartbeat_ms());
        assert_eq!(after.lease_deadline_ms(), before.lease_deadline_ms());
        assert_eq!(
            authority.deterministic_pg_primary(PgId::new(1), &[NodeId::new(7)]),
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
            .heartbeat(heartbeat(2, authority.snapshot().cluster_epoch(), 100))
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
            .heartbeat(heartbeat(2, unavailable_epoch, 500))
            .unwrap();
        assert!(recovered.cluster_epoch() > unavailable_epoch);
        assert!(!recovered.serving());
        let serving = authority
            .heartbeat(heartbeat(2, recovered.cluster_epoch(), 600))
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
            .heartbeat(heartbeat_from_record(
                &authority,
                3,
                authority.snapshot().cluster_epoch(),
                2_000,
            ))
            .unwrap();

        let acting_set = [NodeId::new(1), NodeId::new(2), NodeId::new(3)];
        assert_eq!(
            authority.deterministic_pg_primary(PgId::new(7), &acting_set),
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
            .heartbeat(heartbeat_from_record(
                &authority,
                1,
                authority.snapshot().cluster_epoch(),
                1_002,
            ))
            .unwrap();
        assert!(node_one_current.serving());
        assert_eq!(
            authority.deterministic_pg_primary(PgId::new(1), &[NodeId::new(1), NodeId::new(2)]),
            Some(NodeId::new(1))
        );
        authority
            .set_pg_acting_set(PgId::new(9), vec![NodeId::new(1), NodeId::new(2)])
            .unwrap();
        for node_id in [1, 2] {
            authority
                .heartbeat(heartbeat_from_record(
                    &authority,
                    node_id,
                    authority.snapshot().cluster_epoch(),
                    1_003 + u64::from(node_id),
                ))
                .unwrap();
        }
        authority
            .complete_pg_peering(
                PgId::new(9),
                NodeId::new(1),
                node_incarnation(&authority, 1),
                1_050,
            )
            .unwrap();
        for node_id in [1, 2] {
            authority
                .heartbeat(heartbeat_from_record(
                    &authority,
                    node_id,
                    authority.snapshot().cluster_epoch(),
                    1_010 + u64::from(node_id),
                ))
                .unwrap();
        }
        assert_eq!(
            authority.serving_pg_primary(PgId::new(9)),
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
            authority.deterministic_pg_primary(PgId::new(1), &[NodeId::new(1), NodeId::new(2)]),
            None
        );
        assert_eq!(authority.serving_pg_primary(PgId::new(9)), None);

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
            .heartbeat(heartbeat_from_record(
                &authority,
                3,
                serving.cluster_epoch(),
                1_200,
            ))
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
            .heartbeat(heartbeat_from_record(
                &authority,
                3,
                stale_after_expiry.cluster_epoch(),
                1_300,
            ))
            .unwrap();
        assert!(recovered.cluster_epoch() > stale_after_expiry.cluster_epoch());
        assert!(!recovered.serving());

        let caught_up = authority
            .heartbeat(heartbeat_from_record(
                &authority,
                3,
                recovered.cluster_epoch(),
                1_400,
            ))
            .unwrap();
        assert!(caught_up.serving());
        assert_eq!(
            authority.deterministic_pg_primary(PgId::new(1), &[NodeId::new(3)]),
            Some(NodeId::new(3))
        );
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
            .heartbeat(heartbeat_from_record(
                &restarted,
                12,
                restarted.snapshot().cluster_epoch(),
                1_001,
            ))
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
            restarted.deterministic_pg_primary(PgId::new(1), &[NodeId::new(12)]),
            Some(NodeId::new(12))
        );
    }

    #[test]
    fn heartbeat_rejects_unknown_removed_and_zero_duration_nodes() {
        let tmp = test_util::tempdir();
        let store = FileControlPlaneStore::new(tmp.path().join("control-plane.state"));
        let mut authority = SingleAuthorityControlPlane::open(store).unwrap();
        assert!(matches!(
            authority.heartbeat(heartbeat(9, ClusterEpoch::INITIAL, 1)),
            Err(ControlPlaneError::UnknownNode { node_id: 9 })
        ));

        authority
            .set_node_membership(NodeId::new(9), NodeMembershipState::Removed)
            .unwrap();
        assert!(matches!(
            authority.heartbeat(heartbeat(9, authority.snapshot().cluster_epoch(), 2)),
            Err(ControlPlaneError::NodeCannotReceiveLease { node_id: 9, .. })
        ));

        authority
            .set_node_membership(NodeId::new(10), NodeMembershipState::Active)
            .unwrap();
        let mut invalid = heartbeat(10, authority.snapshot().cluster_epoch(), 3);
        invalid.lease_duration_ms = 0;
        assert!(matches!(
            authority.heartbeat(invalid),
            Err(ControlPlaneError::InvalidLeaseDuration)
        ));
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
