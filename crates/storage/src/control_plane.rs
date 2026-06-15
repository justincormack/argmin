use std::collections::BTreeMap;
use std::io::Write as _;
use std::num::NonZeroU64;
use std::path::{Path, PathBuf};

use placement::NodeId;
use thiserror::Error;

use crate::{ClusterEpoch, PgId};

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
}

impl ClusterControlSnapshot {
    fn empty() -> Self {
        Self {
            authority_incarnation: AuthorityIncarnation::INITIAL,
            cluster_epoch: ClusterEpoch::INITIAL,
            nodes: BTreeMap::new(),
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

    fn bump_authority_after_restart(&mut self) -> Result<(), ControlPlaneError> {
        self.authority_incarnation = self.authority_incarnation.next()?;
        self.cluster_epoch = next_epoch(self.cluster_epoch)?;
        Ok(())
    }

    fn bump_epoch(&mut self) -> Result<(), ControlPlaneError> {
        self.cluster_epoch = next_epoch(self.cluster_epoch)?;
        Ok(())
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
    pub fn snapshot(&self) -> &ClusterControlSnapshot {
        &self.snapshot
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
            snapshot.bump_authority_after_restart()?;
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
            if availability != NodeAvailabilityState::Healthy {
                record.lease_deadline_ms = None;
            }
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
            next_snapshot.bump_epoch()?;
            self.commit_snapshot(next_snapshot)?;
        }
        Ok(HeartbeatLeaseExpiry {
            cluster_epoch: self.snapshot.cluster_epoch,
            expired_nodes,
            snapshot: self.snapshot.clone(),
        })
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
        next_snapshot: ClusterControlSnapshot,
    ) -> Result<(), ControlPlaneError> {
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
    out.push_str("version=1\n");
    out.push_str(&format!(
        "authority_incarnation={}\n",
        snapshot.authority_incarnation.get()
    ));
    out.push_str(&format!("cluster_epoch={}\n", snapshot.cluster_epoch.get()));
    for record in snapshot.nodes.values() {
        out.push_str(&format!(
            "node={},{},{},{},{},{},{},{}\n",
            record.node_id.as_u32(),
            record.membership.as_str(),
            record.availability.as_str(),
            record.node_incarnation,
            option_u64(record.last_observed_epoch.map(ClusterEpoch::get)),
            option_u64(record.last_heartbeat_ms),
            option_u64(record.lease_deadline_ms),
            hex_encode(record.endpoint.as_bytes())
        ));
    }
    out
}

fn parse_snapshot(contents: &str) -> Result<ClusterControlSnapshot, ControlPlaneError> {
    let mut version = None;
    let mut authority_incarnation = None;
    let mut cluster_epoch = None;
    let mut nodes = BTreeMap::new();

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
        } else if let Some(value) = line.strip_prefix("node=") {
            let record = parse_node_record(line_number, value)?;
            if nodes.insert(record.node_id, record).is_some() {
                return Err(parse_error(line_number, "duplicate node record"));
            }
        } else {
            return Err(parse_error(line_number, "unknown control-plane state line"));
        }
    }

    if version != Some(1) {
        return Err(parse_error(
            0,
            "missing or unsupported control-plane state version",
        ));
    }
    Ok(ClusterControlSnapshot {
        authority_incarnation: authority_incarnation
            .ok_or_else(|| parse_error(0, "missing authority incarnation"))?,
        cluster_epoch: cluster_epoch.ok_or_else(|| parse_error(0, "missing cluster epoch"))?,
        nodes,
    })
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

        let before_expiry_epoch = authority.snapshot().cluster_epoch();
        let expiry = authority.expire_heartbeat_leases(1_102).unwrap();
        assert_eq!(expiry.expired_nodes(), &[NodeId::new(1), NodeId::new(2)]);
        assert!(expiry.cluster_epoch() > before_expiry_epoch);
        assert_eq!(expiry.snapshot().cluster_epoch(), expiry.cluster_epoch());
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

        let repeated = authority.expire_heartbeat_leases(9_999).unwrap();
        assert_eq!(repeated.expired_nodes(), &[]);
        assert_eq!(repeated.cluster_epoch(), expiry.cluster_epoch());

        let persisted = store.load().unwrap().unwrap();
        assert_eq!(persisted.cluster_epoch(), expiry.cluster_epoch());
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
