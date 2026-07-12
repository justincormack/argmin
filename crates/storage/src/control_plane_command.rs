use crate::control_plane::{
    format_snapshot, parse_snapshot, validate_control_plane_snapshot, ClusterControlSnapshot,
    ClusterRuntimeMapSnapshot, ControlPlaneError, NodeAvailabilityState, NodeHeartbeat,
    NodeMembershipState, NodePgHeartbeatObservation, PendingMetadataCommandObservation,
    PgMetadataProof, PgMetadataTransferProof, RuntimeMapFreshnessProof,
};
pub use crate::control_plane_lease::LeaseHorizonAuthorityBinding;
use crate::types::{PgId, PgState};
use crate::{
    ClusterEpoch, PgClusterMapHistoryRouteReference, PgClusterMapHistoryRouteReferenceKind,
    PgClusterMapHistoryRouteReferences, MAX_PG_CLUSTER_MAP_HISTORY_ROUTE_REFERENCES,
};
use placement::NodeId;
use std::num::NonZeroU64;

const CONTROL_PLANE_COMMAND_MAGIC: &[u8; 8] = b"ARGCPCMD";
const CONTROL_PLANE_COMMAND_VERSION: u16 = 6;
const CONTROL_PLANE_COMMAND_CHECKSUM_LEN: usize = 8;
const CONTROL_PLANE_SNAPSHOT_MAGIC: &[u8; 8] = b"ARGCPSNP";
const CONTROL_PLANE_SNAPSHOT_VERSION: u16 = 1;
const CONTROL_PLANE_SNAPSHOT_CHECKSUM_LEN: usize = 8;
const CONTROL_PLANE_COMMAND_BOOTSTRAP_NODE_MIN_LEN: usize = 8;
const CONTROL_PLANE_COMMAND_ACTING_SET_NODE_MIN_LEN: usize = 4;
const CONTROL_PLANE_COMMAND_PG_MIN_LEN: usize = 4;
const CONTROL_PLANE_COMMAND_HEARTBEAT_OBSERVATION_MIN_LEN: usize = 30;
const CONTROL_PLANE_COMMAND_HISTORY_ROUTE_REFERENCE_MIN_LEN: usize = 13;
const CONTROL_PLANE_COMMAND_READY_PG_MIN_LEN: usize = 48;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ControlPlaneCommand {
    BootstrapInitialClusterMap {
        nodes: Vec<(NodeId, String)>,
        pg_ids: Vec<PgId>,
    },
    SetNodeMembership {
        node_id: NodeId,
        membership: NodeMembershipState,
    },
    MarkNodeAvailability {
        node_id: NodeId,
        availability: NodeAvailabilityState,
    },
    RecordNodeHeartbeat {
        heartbeat: NodeHeartbeat,
        heartbeat_at_ms: u64,
        lease_deadline_ms: u64,
    },
    ExpireHeartbeatLeases {
        expire_at_ms: u64,
    },
    EstablishLeaseGrantHorizon {
        authority: LeaseHorizonAuthorityBinding,
        authority_now_ms: u64,
        horizon_duration_ms: u64,
    },
    SetPgActingSet {
        pg_id: PgId,
        acting_set: Vec<NodeId>,
    },
    SetPgActingSetWithMetadataTransfer {
        pg_id: PgId,
        acting_set: Vec<NodeId>,
        transfer: PgMetadataTransferProof,
    },
    FencePgForMetadataTransfer {
        pg_id: PgId,
    },
    SetPgState {
        pg_id: PgId,
        state: PgState,
    },
    CompletePgPeering {
        pg_id: PgId,
        primary: NodeId,
        node_incarnation: u64,
        complete_at_ms: u64,
    },
    CompleteReadyPgPeerings {
        ready_at_ms: u64,
        ready: Vec<ReadyPgPeeringCompletion>,
    },
}

impl std::fmt::Display for ControlPlaneCommand {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ControlPlaneCommand::BootstrapInitialClusterMap { nodes, pg_ids } => write!(
                f,
                "bootstrap-initial-cluster-map(nodes={},pgs={})",
                nodes.len(),
                pg_ids.len()
            ),
            ControlPlaneCommand::SetNodeMembership {
                node_id,
                membership,
            } => write!(
                f,
                "set-node-membership(node={},membership={:?})",
                node_id.as_u32(),
                membership
            ),
            ControlPlaneCommand::MarkNodeAvailability {
                node_id,
                availability,
            } => write!(
                f,
                "mark-node-availability(node={},availability={:?})",
                node_id.as_u32(),
                availability
            ),
            ControlPlaneCommand::RecordNodeHeartbeat {
                heartbeat,
                heartbeat_at_ms,
                lease_deadline_ms,
            } => write!(
                f,
                "record-node-heartbeat(node={},heartbeat_at_ms={},lease_deadline_ms={})",
                heartbeat.node_id.as_u32(),
                heartbeat_at_ms,
                lease_deadline_ms
            ),
            ControlPlaneCommand::ExpireHeartbeatLeases { expire_at_ms } => {
                write!(f, "expire-heartbeat-leases(expire_at_ms={expire_at_ms})")
            }
            ControlPlaneCommand::EstablishLeaseGrantHorizon {
                authority,
                authority_now_ms,
                horizon_duration_ms,
            } => write!(
                f,
                "establish-lease-grant-horizon(clock_generation={},raft_term={:?},authority_now_ms={},duration_ms={})",
                authority.clock_generation(),
                authority.raft_term(),
                authority_now_ms,
                horizon_duration_ms
            ),
            ControlPlaneCommand::SetPgActingSet { pg_id, acting_set } => write!(
                f,
                "set-pg-acting-set(pg={},nodes={})",
                pg_id.get(),
                acting_set.len()
            ),
            ControlPlaneCommand::SetPgActingSetWithMetadataTransfer {
                pg_id,
                acting_set,
                transfer,
            } => write!(
                f,
                "set-pg-acting-set-with-metadata-transfer(pg={},nodes={},source_epoch={})",
                pg_id.get(),
                acting_set.len(),
                transfer.source_epoch().get()
            ),
            ControlPlaneCommand::FencePgForMetadataTransfer { pg_id } => {
                write!(f, "fence-pg-for-metadata-transfer(pg={})", pg_id.get())
            }
            ControlPlaneCommand::SetPgState { pg_id, state } => {
                write!(f, "set-pg-state(pg={},state={state:?})", pg_id.get())
            }
            ControlPlaneCommand::CompletePgPeering {
                pg_id,
                primary,
                node_incarnation,
                complete_at_ms,
            } => write!(
                f,
                "complete-pg-peering(pg={},primary={},incarnation={},complete_at_ms={})",
                pg_id.get(),
                primary.as_u32(),
                node_incarnation,
                complete_at_ms
            ),
            ControlPlaneCommand::CompleteReadyPgPeerings { ready_at_ms, ready } => write!(
                f,
                "complete-ready-pg-peerings(ready_at_ms={},pgs={})",
                ready_at_ms,
                ready.len()
            ),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ReadyPgPeeringCompletion {
    pub pg_id: PgId,
    pub primary: NodeId,
    pub node_incarnation: u64,
    pub active_metadata_proof: PgMetadataProof,
    pub active_metadata_proof_epoch: ClusterEpoch,
}

pub fn encode_control_plane_command(
    command: &ControlPlaneCommand,
) -> Result<Vec<u8>, ControlPlaneError> {
    let mut out = Vec::new();
    out.extend_from_slice(CONTROL_PLANE_COMMAND_MAGIC);
    write_u16(&mut out, CONTROL_PLANE_COMMAND_VERSION);
    match command {
        ControlPlaneCommand::BootstrapInitialClusterMap { nodes, pg_ids } => {
            write_u16(&mut out, 1);
            write_u32(&mut out, len_as_u32(nodes.len(), "bootstrap nodes")?);
            for (node_id, endpoint) in nodes {
                write_u32(&mut out, node_id.as_u32());
                write_string(&mut out, endpoint)?;
            }
            write_u32(&mut out, len_as_u32(pg_ids.len(), "bootstrap PGs")?);
            for pg_id in pg_ids {
                write_u32(&mut out, pg_id.get());
            }
        }
        ControlPlaneCommand::SetNodeMembership {
            node_id,
            membership,
        } => {
            write_u16(&mut out, 2);
            write_u32(&mut out, node_id.as_u32());
            write_node_membership(&mut out, *membership);
        }
        ControlPlaneCommand::MarkNodeAvailability {
            node_id,
            availability,
        } => {
            write_u16(&mut out, 3);
            write_u32(&mut out, node_id.as_u32());
            write_node_availability(&mut out, *availability);
        }
        ControlPlaneCommand::RecordNodeHeartbeat {
            heartbeat,
            heartbeat_at_ms,
            lease_deadline_ms,
        } => {
            write_u16(&mut out, 4);
            write_node_heartbeat(&mut out, heartbeat)?;
            write_u64(&mut out, *heartbeat_at_ms);
            write_u64(&mut out, *lease_deadline_ms);
        }
        ControlPlaneCommand::ExpireHeartbeatLeases { expire_at_ms } => {
            write_u16(&mut out, 5);
            write_u64(&mut out, *expire_at_ms);
        }
        ControlPlaneCommand::SetPgActingSet { pg_id, acting_set } => {
            write_u16(&mut out, 6);
            write_pg_acting_set(&mut out, *pg_id, acting_set)?;
        }
        ControlPlaneCommand::SetPgActingSetWithMetadataTransfer {
            pg_id,
            acting_set,
            transfer,
        } => {
            write_u16(&mut out, 7);
            write_pg_acting_set(&mut out, *pg_id, acting_set)?;
            write_pg_metadata_transfer_proof(&mut out, *transfer);
        }
        ControlPlaneCommand::FencePgForMetadataTransfer { pg_id } => {
            write_u16(&mut out, 8);
            write_u32(&mut out, pg_id.get());
        }
        ControlPlaneCommand::SetPgState { pg_id, state } => {
            write_u16(&mut out, 9);
            write_u32(&mut out, pg_id.get());
            write_pg_state(&mut out, *state);
        }
        ControlPlaneCommand::CompletePgPeering {
            pg_id,
            primary,
            node_incarnation,
            complete_at_ms,
        } => {
            write_u16(&mut out, 10);
            write_u32(&mut out, pg_id.get());
            write_u32(&mut out, primary.as_u32());
            write_u64(&mut out, *node_incarnation);
            write_u64(&mut out, *complete_at_ms);
        }
        ControlPlaneCommand::CompleteReadyPgPeerings { ready_at_ms, ready } => {
            write_u16(&mut out, 11);
            write_u64(&mut out, *ready_at_ms);
            write_u32(
                &mut out,
                len_as_u32(ready.len(), "ready PG peering completions")?,
            );
            for completion in ready {
                write_u32(&mut out, completion.pg_id.get());
                write_u32(&mut out, completion.primary.as_u32());
                write_u64(&mut out, completion.node_incarnation);
                write_pg_metadata_proof(&mut out, completion.active_metadata_proof);
                write_u64(&mut out, completion.active_metadata_proof_epoch.get());
            }
        }
        ControlPlaneCommand::EstablishLeaseGrantHorizon {
            authority,
            authority_now_ms,
            horizon_duration_ms,
        } => {
            write_u16(&mut out, 12);
            write_u64(&mut out, authority.clock_generation());
            match authority.raft_term() {
                Some(term) => {
                    write_u8(&mut out, 1);
                    write_u64(&mut out, term);
                }
                None => write_u8(&mut out, 0),
            }
            write_u64(&mut out, *authority_now_ms);
            write_u64(&mut out, *horizon_duration_ms);
        }
    }
    append_control_plane_command_checksum(&mut out);
    Ok(out)
}

pub fn decode_control_plane_command(
    bytes: &[u8],
) -> Result<ControlPlaneCommand, ControlPlaneError> {
    let min_len = CONTROL_PLANE_COMMAND_MAGIC.len()
        + std::mem::size_of::<u16>()
        + std::mem::size_of::<u16>()
        + CONTROL_PLANE_COMMAND_CHECKSUM_LEN;
    if bytes.len() < min_len {
        return Err(command_protocol_error(
            "truncated control-plane command payload",
        ));
    }
    let (body, checksum_bytes) = bytes.split_at(bytes.len() - CONTROL_PLANE_COMMAND_CHECKSUM_LEN);
    let expected_checksum = u64::from_be_bytes(
        checksum_bytes
            .try_into()
            .expect("checksum split length is fixed"),
    );
    let actual_checksum = control_plane_command_checksum(body);
    if actual_checksum != expected_checksum {
        return Err(command_protocol_error(format!(
            "control-plane command checksum mismatch: expected {expected_checksum:#x}, actual {actual_checksum:#x}"
        )));
    }

    let mut reader = PayloadReader::new(body);
    if reader.read_exact(CONTROL_PLANE_COMMAND_MAGIC.len())? != CONTROL_PLANE_COMMAND_MAGIC {
        return Err(command_protocol_error(
            "invalid control-plane command magic",
        ));
    }
    let version = reader.read_u16()?;
    if version != CONTROL_PLANE_COMMAND_VERSION {
        return Err(command_protocol_error(format!(
            "unsupported control-plane command version {version}"
        )));
    }
    let command = match reader.read_u16()? {
        1 => {
            let node_count = reader.read_collection_len(
                "bootstrap nodes",
                CONTROL_PLANE_COMMAND_BOOTSTRAP_NODE_MIN_LEN,
            )?;
            let mut nodes = Vec::with_capacity(node_count);
            for _ in 0..node_count {
                nodes.push((
                    NodeId::new(reader.read_u32()?),
                    reader.read_string()?.to_owned(),
                ));
            }
            let pg_count =
                reader.read_collection_len("bootstrap PGs", CONTROL_PLANE_COMMAND_PG_MIN_LEN)?;
            let mut pg_ids = Vec::with_capacity(pg_count);
            for _ in 0..pg_count {
                pg_ids.push(PgId::new(reader.read_u32()?));
            }
            ControlPlaneCommand::BootstrapInitialClusterMap { nodes, pg_ids }
        }
        2 => ControlPlaneCommand::SetNodeMembership {
            node_id: NodeId::new(reader.read_u32()?),
            membership: read_node_membership(&mut reader)?,
        },
        3 => ControlPlaneCommand::MarkNodeAvailability {
            node_id: NodeId::new(reader.read_u32()?),
            availability: read_node_availability(&mut reader)?,
        },
        4 => ControlPlaneCommand::RecordNodeHeartbeat {
            heartbeat: read_node_heartbeat(&mut reader)?,
            heartbeat_at_ms: reader.read_u64()?,
            lease_deadline_ms: reader.read_u64()?,
        },
        5 => ControlPlaneCommand::ExpireHeartbeatLeases {
            expire_at_ms: reader.read_u64()?,
        },
        6 => {
            let (pg_id, acting_set) = read_pg_acting_set(&mut reader)?;
            ControlPlaneCommand::SetPgActingSet { pg_id, acting_set }
        }
        7 => {
            let (pg_id, acting_set) = read_pg_acting_set(&mut reader)?;
            let transfer = read_pg_metadata_transfer_proof(&mut reader)?;
            ControlPlaneCommand::SetPgActingSetWithMetadataTransfer {
                pg_id,
                acting_set,
                transfer,
            }
        }
        8 => ControlPlaneCommand::FencePgForMetadataTransfer {
            pg_id: PgId::new(reader.read_u32()?),
        },
        9 => ControlPlaneCommand::SetPgState {
            pg_id: PgId::new(reader.read_u32()?),
            state: read_pg_state(&mut reader)?,
        },
        10 => ControlPlaneCommand::CompletePgPeering {
            pg_id: PgId::new(reader.read_u32()?),
            primary: NodeId::new(reader.read_u32()?),
            node_incarnation: reader.read_u64()?,
            complete_at_ms: reader.read_u64()?,
        },
        11 => {
            let ready_at_ms = reader.read_u64()?;
            let ready_count = reader.read_collection_len(
                "ready PG peering completions",
                CONTROL_PLANE_COMMAND_READY_PG_MIN_LEN,
            )?;
            let mut ready = Vec::with_capacity(ready_count);
            for _ in 0..ready_count {
                ready.push(ReadyPgPeeringCompletion {
                    pg_id: PgId::new(reader.read_u32()?),
                    primary: NodeId::new(reader.read_u32()?),
                    node_incarnation: reader.read_u64()?,
                    active_metadata_proof: read_pg_metadata_proof(&mut reader)?,
                    active_metadata_proof_epoch: read_cluster_epoch(
                        &mut reader,
                        "ready PG peering proof epoch",
                    )?,
                });
            }
            ControlPlaneCommand::CompleteReadyPgPeerings { ready_at_ms, ready }
        }
        12 => {
            let clock_generation = reader.read_u64()?;
            let raft_term = match reader.read_u8()? {
                0 => None,
                1 => Some(reader.read_u64()?),
                tag => {
                    return Err(command_protocol_error(format!(
                        "invalid lease horizon Raft term option tag {tag}"
                    )));
                }
            };
            let authority = LeaseHorizonAuthorityBinding::checked_new(clock_generation, raft_term)
                .ok_or_else(|| {
                    command_protocol_error(
                        "lease horizon clock generation and present Raft term must be nonzero",
                    )
                })?;
            ControlPlaneCommand::EstablishLeaseGrantHorizon {
                authority,
                authority_now_ms: reader.read_u64()?,
                horizon_duration_ms: reader.read_u64()?,
            }
        }
        tag => {
            return Err(command_protocol_error(format!(
                "unknown control-plane command tag {tag}"
            )));
        }
    };
    reader.finish()?;
    Ok(command)
}

pub fn encode_control_plane_snapshot(
    snapshot: &ClusterControlSnapshot,
) -> Result<Vec<u8>, ControlPlaneError> {
    let mut out = Vec::new();
    out.extend_from_slice(CONTROL_PLANE_SNAPSHOT_MAGIC);
    write_u16(&mut out, CONTROL_PLANE_SNAPSHOT_VERSION);
    write_string(&mut out, &format_snapshot(snapshot))?;
    append_control_plane_snapshot_checksum(&mut out);
    Ok(out)
}

pub fn decode_control_plane_snapshot(
    bytes: &[u8],
) -> Result<ClusterControlSnapshot, ControlPlaneError> {
    let min_len = CONTROL_PLANE_SNAPSHOT_MAGIC.len()
        + std::mem::size_of::<u16>()
        + std::mem::size_of::<u32>()
        + CONTROL_PLANE_SNAPSHOT_CHECKSUM_LEN;
    if bytes.len() < min_len {
        return Err(snapshot_protocol_error(
            "truncated control-plane snapshot payload",
        ));
    }
    let (body, checksum_bytes) = bytes.split_at(bytes.len() - CONTROL_PLANE_SNAPSHOT_CHECKSUM_LEN);
    let expected_checksum = u64::from_be_bytes(
        checksum_bytes
            .try_into()
            .expect("checksum split length is fixed"),
    );
    let actual_checksum = control_plane_snapshot_checksum(body);
    if actual_checksum != expected_checksum {
        return Err(snapshot_protocol_error(format!(
            "control-plane snapshot checksum mismatch: expected {expected_checksum:#x}, actual {actual_checksum:#x}"
        )));
    }

    let version_offset = CONTROL_PLANE_SNAPSHOT_MAGIC.len();
    if body.get(..version_offset) != Some(CONTROL_PLANE_SNAPSHOT_MAGIC) {
        return Err(snapshot_protocol_error(
            "invalid control-plane snapshot magic",
        ));
    }
    let version = u16::from_be_bytes(
        body.get(version_offset..version_offset + std::mem::size_of::<u16>())
            .expect("snapshot body length already checked")
            .try_into()
            .expect("version slice length is fixed"),
    );
    if version != CONTROL_PLANE_SNAPSHOT_VERSION {
        return Err(snapshot_protocol_error(format!(
            "unsupported control-plane snapshot version {version}"
        )));
    }
    let len_offset = version_offset + std::mem::size_of::<u16>();
    let content_offset = len_offset + std::mem::size_of::<u32>();
    let content_len = usize::try_from(u32::from_be_bytes(
        body.get(len_offset..content_offset)
            .expect("snapshot body length already checked")
            .try_into()
            .expect("length slice length is fixed"),
    ))
    .map_err(|_| snapshot_protocol_error("snapshot content length does not fit usize"))?;
    let expected_len = content_offset
        .checked_add(content_len)
        .ok_or_else(|| snapshot_protocol_error("snapshot content length overflow"))?;
    if expected_len != body.len() {
        return Err(snapshot_protocol_error(format!(
            "control-plane snapshot payload length {content_len} does not match frame body length {}",
            body.len() - content_offset
        )));
    }
    let contents = std::str::from_utf8(
        body.get(content_offset..expected_len)
            .expect("snapshot content range already checked"),
    )
    .map_err(|source| {
        snapshot_protocol_error(format!(
            "control-plane snapshot content is not UTF-8: {source}"
        ))
    })?;
    parse_snapshot(contents)
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ControlPlaneCommandResponse {
    BootstrapInitialClusterMap,
    SetNodeMembership,
    MarkNodeAvailability,
    RecordNodeHeartbeat,
    EstablishLeaseGrantHorizon,
    ExpireHeartbeatLeases {
        expired_nodes: Vec<NodeId>,
        peering_pgs: Vec<PgId>,
    },
    SetPgActingSet,
    SetPgActingSetWithMetadataTransfer,
    FencePgForMetadataTransfer {
        source_primary_lease_deadline_ms: Option<u64>,
    },
    SetPgState,
    CompletePgPeering,
    CompleteReadyPgPeerings,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AppliedControlPlaneCommand {
    snapshot: ClusterControlSnapshot,
    response: ControlPlaneCommandResponse,
    changed: bool,
}

impl AppliedControlPlaneCommand {
    #[must_use]
    pub(crate) fn new(
        snapshot: ClusterControlSnapshot,
        response: ControlPlaneCommandResponse,
        changed: bool,
    ) -> Self {
        Self {
            snapshot,
            response,
            changed,
        }
    }

    #[must_use]
    pub fn snapshot(&self) -> &ClusterControlSnapshot {
        &self.snapshot
    }

    #[must_use]
    pub fn response(&self) -> &ControlPlaneCommandResponse {
        &self.response
    }

    #[must_use]
    pub fn changed(&self) -> bool {
        self.changed
    }

    #[must_use]
    pub fn into_snapshot(self) -> ClusterControlSnapshot {
        self.snapshot
    }
}

pub trait ControlPlaneCommandStateMachine {
    fn apply_control_plane_command(
        &self,
        command: ControlPlaneCommand,
    ) -> Result<AppliedControlPlaneCommand, ControlPlaneError>;
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub struct ControlPlaneLogId {
    term: u64,
    index: u64,
}

impl ControlPlaneLogId {
    #[must_use]
    pub fn new(term: u64, index: u64) -> Option<Self> {
        if term == 0 || index == 0 {
            return None;
        }
        Some(Self { term, index })
    }

    #[must_use]
    pub fn term(self) -> u64 {
        self.term
    }

    #[must_use]
    pub fn index(self) -> u64 {
        self.index
    }

    fn next_index(self) -> Result<u64, ControlPlaneError> {
        self.index
            .checked_add(1)
            .ok_or(ControlPlaneError::ControlPlaneLogIndexOverflow { index: self.index })
    }
}

#[derive(Debug)]
pub enum CommittedControlPlaneCommandOutcome {
    Applied(AppliedControlPlaneCommand),
    Rejected(ControlPlaneError),
}

#[derive(Debug)]
pub struct CommittedControlPlaneLogCommand {
    log_id: ControlPlaneLogId,
    outcome: CommittedControlPlaneCommandOutcome,
}

impl CommittedControlPlaneLogCommand {
    #[must_use]
    pub fn applied(log_id: ControlPlaneLogId, applied: AppliedControlPlaneCommand) -> Self {
        Self {
            log_id,
            outcome: CommittedControlPlaneCommandOutcome::Applied(applied),
        }
    }

    #[must_use]
    pub fn rejected(log_id: ControlPlaneLogId, error: ControlPlaneError) -> Self {
        Self {
            log_id,
            outcome: CommittedControlPlaneCommandOutcome::Rejected(error),
        }
    }

    #[must_use]
    pub fn log_id(&self) -> ControlPlaneLogId {
        self.log_id
    }

    #[must_use]
    pub fn outcome(&self) -> &CommittedControlPlaneCommandOutcome {
        &self.outcome
    }

    #[must_use]
    pub fn applied_command(&self) -> Option<&AppliedControlPlaneCommand> {
        match &self.outcome {
            CommittedControlPlaneCommandOutcome::Applied(applied) => Some(applied),
            CommittedControlPlaneCommandOutcome::Rejected(_) => None,
        }
    }

    #[must_use]
    pub fn rejection(&self) -> Option<&ControlPlaneError> {
        match &self.outcome {
            CommittedControlPlaneCommandOutcome::Applied(_) => None,
            CommittedControlPlaneCommandOutcome::Rejected(error) => Some(error),
        }
    }

    #[must_use]
    pub fn into_outcome(self) -> CommittedControlPlaneCommandOutcome {
        self.outcome
    }

    /// Convenience for callers that have already established that this
    /// committed entry applied. Consensus wiring must handle `Rejected`
    /// explicitly through `outcome` or `rejection` so deterministic command
    /// rejection is not re-propagated as log-application failure.
    pub fn into_applied_command(self) -> Result<AppliedControlPlaneCommand, ControlPlaneError> {
        match self.outcome {
            CommittedControlPlaneCommandOutcome::Applied(applied) => Ok(applied),
            CommittedControlPlaneCommandOutcome::Rejected(error) => Err(error),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ControlPlaneSnapshotArtifact {
    last_applied: Option<ControlPlaneLogId>,
    payload: Vec<u8>,
}

impl ControlPlaneSnapshotArtifact {
    #[must_use]
    pub fn new(last_applied: Option<ControlPlaneLogId>, payload: Vec<u8>) -> Self {
        Self {
            last_applied,
            payload,
        }
    }

    #[must_use]
    pub fn last_applied(&self) -> Option<ControlPlaneLogId> {
        self.last_applied
    }

    #[must_use]
    pub fn payload(&self) -> &[u8] {
        &self.payload
    }

    #[must_use]
    pub fn into_payload(self) -> Vec<u8> {
        self.payload
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReplicatedControlPlaneStateMachine {
    snapshot: ClusterControlSnapshot,
    last_applied: Option<ControlPlaneLogId>,
    snapshot_last_applied: Option<ControlPlaneLogId>,
}

impl ReplicatedControlPlaneStateMachine {
    #[must_use]
    pub fn empty() -> Self {
        Self {
            snapshot: ClusterControlSnapshot::empty(),
            last_applied: None,
            snapshot_last_applied: None,
        }
    }

    pub fn new(
        snapshot: ClusterControlSnapshot,
        last_applied: Option<ControlPlaneLogId>,
    ) -> Result<Self, ControlPlaneError> {
        validate_control_plane_snapshot(
            "attempted to create replicated state machine from invalid control-plane snapshot",
            &snapshot,
        )?;
        Ok(Self {
            snapshot,
            last_applied,
            snapshot_last_applied: None,
        })
    }

    #[must_use]
    pub fn snapshot(&self) -> &ClusterControlSnapshot {
        &self.snapshot
    }

    #[must_use]
    pub fn last_applied(&self) -> Option<ControlPlaneLogId> {
        self.last_applied
    }

    #[must_use]
    pub fn snapshot_last_applied(&self) -> Option<ControlPlaneLogId> {
        self.snapshot_last_applied
    }

    pub fn runtime_map_for_read_index(
        &self,
        read_index: ControlPlaneLogId,
        issued_at_ms: u64,
    ) -> Result<ClusterRuntimeMapSnapshot, ControlPlaneError> {
        self.validate_read_index_applied(read_index)?;
        self.snapshot.runtime_map_with_freshness_proof(
            issued_at_ms,
            RuntimeMapFreshnessProof::ReadIndex {
                authority_incarnation: self.snapshot.authority_incarnation(),
                read_index,
                issued_at_ms,
            },
        )
    }

    pub fn apply_committed_command(
        &mut self,
        log_id: ControlPlaneLogId,
        command: ControlPlaneCommand,
    ) -> Result<CommittedControlPlaneLogCommand, ControlPlaneError> {
        self.validate_next_log_id(log_id)?;
        match self.snapshot.apply_control_plane_command(command) {
            Ok(applied) => {
                self.last_applied = Some(log_id);
                self.snapshot = applied.snapshot().clone();
                Ok(CommittedControlPlaneLogCommand::applied(log_id, applied))
            }
            Err(error @ ControlPlaneError::SnapshotInvariantViolation { .. }) => Err(error),
            Err(error) => {
                self.last_applied = Some(log_id);
                Ok(CommittedControlPlaneLogCommand::rejected(log_id, error))
            }
        }
    }

    pub fn apply_committed_noop(
        &mut self,
        log_id: ControlPlaneLogId,
    ) -> Result<(), ControlPlaneError> {
        self.validate_next_log_id(log_id)?;
        self.last_applied = Some(log_id);
        Ok(())
    }

    pub fn build_snapshot_artifact(
        &mut self,
    ) -> Result<ControlPlaneSnapshotArtifact, ControlPlaneError> {
        let payload = encode_control_plane_snapshot(&self.snapshot)?;
        self.snapshot_last_applied = self.last_applied;
        Ok(ControlPlaneSnapshotArtifact::new(
            self.last_applied,
            payload,
        ))
    }

    pub fn install_snapshot_artifact(
        &mut self,
        artifact: ControlPlaneSnapshotArtifact,
    ) -> Result<(), ControlPlaneError> {
        self.validate_snapshot_artifact_log_id(artifact.last_applied())?;
        // The consensus layer must supply a mutually consistent
        // (payload, last_applied) pair. This adapter guards only against
        // rollback relative to the current applied position.
        let snapshot = decode_control_plane_snapshot(artifact.payload())?;
        validate_control_plane_snapshot(
            "attempted to install invalid replicated control-plane snapshot",
            &snapshot,
        )?;
        self.snapshot = snapshot;
        self.last_applied = artifact.last_applied();
        self.snapshot_last_applied = artifact.last_applied();
        Ok(())
    }

    fn validate_read_index_applied(
        &self,
        read_index: ControlPlaneLogId,
    ) -> Result<(), ControlPlaneError> {
        if self.last_applied == Some(read_index) {
            return Ok(());
        }
        Err(ControlPlaneError::ControlPlaneReadIndexNotApplied {
            read_index,
            last_applied: self.last_applied,
        })
    }

    fn validate_next_log_id(&self, log_id: ControlPlaneLogId) -> Result<(), ControlPlaneError> {
        let Some(last_applied) = self.last_applied else {
            if log_id.index() != 1 {
                return Err(ControlPlaneError::ControlPlaneLogIndexMismatch {
                    expected_index: 1,
                    actual_index: log_id.index(),
                });
            }
            return Ok(());
        };
        let expected_index = last_applied.next_index()?;
        if log_id.index() != expected_index {
            return Err(ControlPlaneError::ControlPlaneLogIndexMismatch {
                expected_index,
                actual_index: log_id.index(),
            });
        }
        if log_id.term() < last_applied.term() {
            return Err(ControlPlaneError::ControlPlaneLogTermRegression {
                previous_term: last_applied.term(),
                actual_term: log_id.term(),
                index: log_id.index(),
            });
        }
        Ok(())
    }

    fn validate_snapshot_artifact_log_id(
        &self,
        artifact_last_applied: Option<ControlPlaneLogId>,
    ) -> Result<(), ControlPlaneError> {
        let Some(current_last_applied) = self.last_applied else {
            return Ok(());
        };
        let Some(artifact_last_applied) = artifact_last_applied else {
            return Err(ControlPlaneError::ControlPlaneSnapshotMissingLogId {
                current_index: current_last_applied.index(),
            });
        };
        if artifact_last_applied.index() < current_last_applied.index() {
            return Err(ControlPlaneError::ControlPlaneSnapshotLogIndexRegression {
                current_index: current_last_applied.index(),
                artifact_index: artifact_last_applied.index(),
            });
        }
        if artifact_last_applied.index() == current_last_applied.index()
            && artifact_last_applied.term() != current_last_applied.term()
        {
            return Err(ControlPlaneError::ControlPlaneSnapshotLogTermMismatch {
                index: current_last_applied.index(),
                current_term: current_last_applied.term(),
                artifact_term: artifact_last_applied.term(),
            });
        }
        if artifact_last_applied.term() < current_last_applied.term() {
            return Err(ControlPlaneError::ControlPlaneLogTermRegression {
                previous_term: current_last_applied.term(),
                actual_term: artifact_last_applied.term(),
                index: artifact_last_applied.index(),
            });
        }
        Ok(())
    }
}

fn append_control_plane_command_checksum(out: &mut Vec<u8>) {
    let checksum = control_plane_command_checksum(out);
    write_u64(out, checksum);
}

fn control_plane_command_checksum(body: &[u8]) -> u64 {
    checksum::crc64::checksum(body)
}

fn append_control_plane_snapshot_checksum(out: &mut Vec<u8>) {
    let checksum = control_plane_snapshot_checksum(out);
    write_u64(out, checksum);
}

fn control_plane_snapshot_checksum(body: &[u8]) -> u64 {
    checksum::crc64::checksum(body)
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
    write_cluster_map_history_route_references(
        out,
        &heartbeat.cluster_map_history_route_references,
    )?;
    write_u32(
        out,
        len_as_u32(heartbeat.pg_observations.len(), "PG observations")?,
    );
    for observation in &heartbeat.pg_observations {
        write_u32(out, observation.pg_id.get());
        write_pg_state(out, observation.state);
        write_pg_metadata_proof(out, observation.metadata_proof);
        write_pending_metadata_command_observation(out, observation.pending_metadata_command);
    }
    Ok(())
}

fn read_node_heartbeat(reader: &mut PayloadReader<'_>) -> Result<NodeHeartbeat, ControlPlaneError> {
    let node_id = NodeId::new(reader.read_u32()?);
    let node_incarnation = reader.read_u64()?;
    let endpoint = reader.read_string()?.to_owned();
    let observed_epoch = read_cluster_epoch(reader, "heartbeat observed epoch")?;
    let requested_lease_duration_ms = reader.read_u64()?;
    let cluster_map_history_route_references = read_cluster_map_history_route_references(reader)?;
    let observation_count = reader.read_collection_len(
        "PG observations",
        CONTROL_PLANE_COMMAND_HEARTBEAT_OBSERVATION_MIN_LEN,
    )?;
    let mut pg_observations = Vec::with_capacity(observation_count);
    for _ in 0..observation_count {
        pg_observations.push(NodePgHeartbeatObservation {
            pg_id: PgId::new(reader.read_u32()?),
            state: read_pg_state(reader)?,
            metadata_proof: read_pg_metadata_proof(reader)?,
            pending_metadata_command: read_pending_metadata_command_observation(reader)?,
        });
    }
    Ok(NodeHeartbeat {
        node_id,
        node_incarnation,
        endpoint,
        observed_epoch,
        requested_lease_duration_ms,
        cluster_map_history_route_references,
        pg_observations,
    })
}

fn write_cluster_map_history_route_references(
    out: &mut Vec<u8>,
    references: &PgClusterMapHistoryRouteReferences,
) -> Result<(), ControlPlaneError> {
    write_u32(
        out,
        len_as_u32(references.len(), "cluster-map history route references")?,
    );
    for reference in references.iter() {
        write_u8(
            out,
            cluster_map_history_route_reference_kind_code(reference.kind()),
        );
        write_u64(out, reference.cluster_epoch().get());
        write_u32(out, reference.pg_id().get());
    }
    Ok(())
}

fn read_cluster_map_history_route_references(
    reader: &mut PayloadReader<'_>,
) -> Result<PgClusterMapHistoryRouteReferences, ControlPlaneError> {
    let count = reader.read_collection_len(
        "cluster-map history route references",
        CONTROL_PLANE_COMMAND_HISTORY_ROUTE_REFERENCE_MIN_LEN,
    )?;
    if count > MAX_PG_CLUSTER_MAP_HISTORY_ROUTE_REFERENCES {
        return Err(ControlPlaneError::CommandDecode {
            message: format!(
                "cluster-map history route reference count {count} exceeds {}",
                MAX_PG_CLUSTER_MAP_HISTORY_ROUTE_REFERENCES
            ),
        });
    }
    let mut decoded = Vec::with_capacity(count);
    let mut previous = None;
    for _ in 0..count {
        let reference = PgClusterMapHistoryRouteReference::new(
            read_cluster_map_history_route_reference_kind(reader)?,
            read_cluster_epoch(reader, "cluster-map history route reference epoch")?,
            PgId::new(reader.read_u32()?),
        );
        if previous.is_some_and(|previous| reference <= previous) {
            return Err(ControlPlaneError::CommandDecode {
                message: "cluster-map history route references are not in canonical order"
                    .to_owned(),
            });
        }
        previous = Some(reference);
        decoded.push(reference);
    }
    PgClusterMapHistoryRouteReferences::try_from_iter(decoded).map_err(|error| {
        ControlPlaneError::CommandDecode {
            message: format!("invalid cluster-map history route references: {error}"),
        }
    })
}

const fn cluster_map_history_route_reference_kind_code(
    kind: PgClusterMapHistoryRouteReferenceKind,
) -> u8 {
    match kind {
        PgClusterMapHistoryRouteReferenceKind::LivePlacement => 1,
        PgClusterMapHistoryRouteReferenceKind::DurableBackfillSource => 2,
        PgClusterMapHistoryRouteReferenceKind::DurableBackfillDesired => 3,
        PgClusterMapHistoryRouteReferenceKind::PendingMetadataCommand => 4,
    }
}

fn read_cluster_map_history_route_reference_kind(
    reader: &mut PayloadReader<'_>,
) -> Result<PgClusterMapHistoryRouteReferenceKind, ControlPlaneError> {
    match reader.read_u8()? {
        1 => Ok(PgClusterMapHistoryRouteReferenceKind::LivePlacement),
        2 => Ok(PgClusterMapHistoryRouteReferenceKind::DurableBackfillSource),
        3 => Ok(PgClusterMapHistoryRouteReferenceKind::DurableBackfillDesired),
        4 => Ok(PgClusterMapHistoryRouteReferenceKind::PendingMetadataCommand),
        value => Err(ControlPlaneError::CommandDecode {
            message: format!("invalid cluster-map history route reference kind {value}"),
        }),
    }
}

fn write_pg_acting_set(
    out: &mut Vec<u8>,
    pg_id: PgId,
    acting_set: &[NodeId],
) -> Result<(), ControlPlaneError> {
    write_u32(out, pg_id.get());
    write_u32(out, len_as_u32(acting_set.len(), "acting set")?);
    for node_id in acting_set {
        write_u32(out, node_id.as_u32());
    }
    Ok(())
}

fn read_pg_acting_set(
    reader: &mut PayloadReader<'_>,
) -> Result<(PgId, Vec<NodeId>), ControlPlaneError> {
    let pg_id = PgId::new(reader.read_u32()?);
    let node_count =
        reader.read_collection_len("acting set", CONTROL_PLANE_COMMAND_ACTING_SET_NODE_MIN_LEN)?;
    let mut acting_set = Vec::with_capacity(node_count);
    for _ in 0..node_count {
        acting_set.push(NodeId::new(reader.read_u32()?));
    }
    Ok((pg_id, acting_set))
}

fn write_pg_metadata_transfer_proof(out: &mut Vec<u8>, transfer: PgMetadataTransferProof) {
    write_u64(out, transfer.source_epoch().get());
    write_pg_metadata_proof(out, transfer.source_metadata_proof());
    write_pg_metadata_proof(out, transfer.metadata_proof());
}

fn read_pg_metadata_transfer_proof(
    reader: &mut PayloadReader<'_>,
) -> Result<PgMetadataTransferProof, ControlPlaneError> {
    let source_epoch = read_cluster_epoch(reader, "metadata transfer source epoch")?;
    let source_metadata_proof = read_pg_metadata_proof(reader)?;
    let imported_metadata_proof = read_pg_metadata_proof(reader)?;
    Ok(PgMetadataTransferProof::new_with_imported_metadata_proof(
        source_epoch,
        source_metadata_proof,
        imported_metadata_proof,
    ))
}

fn write_node_membership(out: &mut Vec<u8>, membership: NodeMembershipState) {
    write_u8(
        out,
        match membership {
            NodeMembershipState::Joining => 1,
            NodeMembershipState::Active => 2,
            NodeMembershipState::Draining => 3,
            NodeMembershipState::Out => 4,
            NodeMembershipState::Removed => 5,
        },
    );
}

fn read_node_membership(
    reader: &mut PayloadReader<'_>,
) -> Result<NodeMembershipState, ControlPlaneError> {
    match reader.read_u8()? {
        1 => Ok(NodeMembershipState::Joining),
        2 => Ok(NodeMembershipState::Active),
        3 => Ok(NodeMembershipState::Draining),
        4 => Ok(NodeMembershipState::Out),
        5 => Ok(NodeMembershipState::Removed),
        state => Err(command_protocol_error(format!(
            "invalid node membership state code {state}"
        ))),
    }
}

fn write_node_availability(out: &mut Vec<u8>, availability: NodeAvailabilityState) {
    write_u8(
        out,
        match availability {
            NodeAvailabilityState::Healthy => 1,
            NodeAvailabilityState::Suspect => 2,
            NodeAvailabilityState::Unavailable => 3,
        },
    );
}

fn read_node_availability(
    reader: &mut PayloadReader<'_>,
) -> Result<NodeAvailabilityState, ControlPlaneError> {
    match reader.read_u8()? {
        1 => Ok(NodeAvailabilityState::Healthy),
        2 => Ok(NodeAvailabilityState::Suspect),
        3 => Ok(NodeAvailabilityState::Unavailable),
        state => Err(command_protocol_error(format!(
            "invalid node availability state code {state}"
        ))),
    }
}

fn write_pg_metadata_proof(out: &mut Vec<u8>, proof: PgMetadataProof) {
    write_u64(out, proof.applied_log_index);
    write_u64(out, proof.applied_log_hash);
    write_u64(out, proof.state_digest);
}

fn write_pending_metadata_command_observation(
    out: &mut Vec<u8>,
    pending: Option<PendingMetadataCommandObservation>,
) {
    match pending {
        Some(pending) => {
            write_u8(out, 1);
            write_u64(out, pending.cluster_epoch().get());
            write_u64(out, pending.log_index());
            write_u64(out, pending.command_checksum());
        }
        None => write_u8(out, 0),
    }
}

fn read_pending_metadata_command_observation(
    reader: &mut PayloadReader<'_>,
) -> Result<Option<PendingMetadataCommandObservation>, ControlPlaneError> {
    match reader.read_u8()? {
        0 => Ok(None),
        1 => {
            let cluster_epoch =
                read_cluster_epoch(reader, "pending metadata command cluster epoch")?;
            let log_index = NonZeroU64::new(reader.read_u64()?).ok_or_else(|| {
                command_protocol_error("pending metadata command log index must be nonzero")
            })?;
            let command_checksum = reader.read_u64()?;
            Ok(Some(PendingMetadataCommandObservation::new(
                cluster_epoch,
                log_index,
                command_checksum,
            )))
        }
        present => Err(command_protocol_error(format!(
            "invalid pending metadata command presence code {present}"
        ))),
    }
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
        state => Err(command_protocol_error(format!(
            "invalid PG state code {state}"
        ))),
    }
}

fn read_cluster_epoch(
    reader: &mut PayloadReader<'_>,
    field: &'static str,
) -> Result<ClusterEpoch, ControlPlaneError> {
    ClusterEpoch::new(reader.read_u64()?)
        .ok_or_else(|| command_protocol_error(format!("{field} must be nonzero")))
}

fn write_string(out: &mut Vec<u8>, value: &str) -> Result<(), ControlPlaneError> {
    write_u32(out, len_as_u32(value.len(), "string")?);
    out.extend_from_slice(value.as_bytes());
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
    u32::try_from(len)
        .map_err(|_| command_protocol_error(format!("{field} length {len} exceeds u32::MAX")))
}

fn command_protocol_error(message: impl Into<String>) -> ControlPlaneError {
    ControlPlaneError::CommandDecode {
        message: message.into(),
    }
}

fn snapshot_protocol_error(message: impl Into<String>) -> ControlPlaneError {
    ControlPlaneError::SnapshotDecode {
        message: message.into(),
    }
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
            Err(command_protocol_error(format!(
                "control-plane command payload has {} trailing bytes",
                self.payload.len() - self.offset
            )))
        }
    }

    fn read_exact(&mut self, len: usize) -> Result<&'a [u8], ControlPlaneError> {
        let end = self.offset.checked_add(len).ok_or_else(|| {
            command_protocol_error("control-plane command payload offset overflow")
        })?;
        let bytes = self
            .payload
            .get(self.offset..end)
            .ok_or_else(|| command_protocol_error("truncated control-plane command payload"))?;
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

    fn read_len(&mut self, field: &'static str) -> Result<usize, ControlPlaneError> {
        usize::try_from(self.read_u32()?)
            .map_err(|_| command_protocol_error(format!("{field} length does not fit usize")))
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
            return Err(command_protocol_error(format!(
                "{field} count {len} exceeds remaining control-plane command payload capacity {max_items}",
            )));
        }
        Ok(len)
    }

    fn read_string(&mut self) -> Result<&'a str, ControlPlaneError> {
        let len = self.read_len("string")?;
        std::str::from_utf8(self.read_exact(len)?).map_err(|source| {
            command_protocol_error(format!(
                "control-plane command string is not UTF-8: {source}"
            ))
        })
    }

    fn remaining_len(&self) -> usize {
        self.payload.len() - self.offset
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample_commands() -> Vec<ControlPlaneCommand> {
        let proof = PgMetadataProof {
            applied_log_index: 7,
            applied_log_hash: 8,
            state_digest: 9,
        };
        let transfer = PgMetadataTransferProof::new_with_imported_metadata_proof(
            ClusterEpoch::new(11).unwrap(),
            PgMetadataProof {
                applied_log_index: 1,
                applied_log_hash: 2,
                state_digest: 3,
            },
            PgMetadataProof {
                applied_log_index: 4,
                applied_log_hash: 5,
                state_digest: 6,
            },
        );
        vec![
            ControlPlaneCommand::BootstrapInitialClusterMap {
                nodes: vec![
                    (NodeId::new(1), "/tmp/node-1.sock".to_owned()),
                    (NodeId::new(2), "/tmp/node-2.sock".to_owned()),
                ],
                pg_ids: vec![PgId::new(3), PgId::new(4)],
            },
            ControlPlaneCommand::SetNodeMembership {
                node_id: NodeId::new(1),
                membership: NodeMembershipState::Draining,
            },
            ControlPlaneCommand::MarkNodeAvailability {
                node_id: NodeId::new(2),
                availability: NodeAvailabilityState::Unavailable,
            },
            ControlPlaneCommand::RecordNodeHeartbeat {
                heartbeat: NodeHeartbeat {
                    node_id: NodeId::new(1),
                    node_incarnation: 12,
                    endpoint: "/tmp/node-1b.sock".to_owned(),
                    observed_epoch: ClusterEpoch::new(13).unwrap(),
                    requested_lease_duration_ms: 100,
                    cluster_map_history_route_references:
                        PgClusterMapHistoryRouteReferences::try_from_iter([
                            PgClusterMapHistoryRouteReference::new(
                                PgClusterMapHistoryRouteReferenceKind::LivePlacement,
                                ClusterEpoch::new(12).unwrap(),
                                PgId::new(3),
                            ),
                            PgClusterMapHistoryRouteReference::new(
                                PgClusterMapHistoryRouteReferenceKind::DurableBackfillSource,
                                ClusterEpoch::new(10).unwrap(),
                                PgId::new(4),
                            ),
                            PgClusterMapHistoryRouteReference::new(
                                PgClusterMapHistoryRouteReferenceKind::DurableBackfillDesired,
                                ClusterEpoch::new(13).unwrap(),
                                PgId::new(4),
                            ),
                            PgClusterMapHistoryRouteReference::new(
                                PgClusterMapHistoryRouteReferenceKind::PendingMetadataCommand,
                                ClusterEpoch::new(11).unwrap(),
                                PgId::new(3),
                            ),
                        ])
                        .unwrap(),
                    pg_observations: vec![NodePgHeartbeatObservation {
                        pg_id: PgId::new(3),
                        state: PgState::Peering,
                        metadata_proof: proof,
                        pending_metadata_command: Some(PendingMetadataCommandObservation::new(
                            ClusterEpoch::new(13).unwrap(),
                            NonZeroU64::new(7).unwrap(),
                            0xfeed,
                        )),
                    }],
                },
                heartbeat_at_ms: 1_000,
                lease_deadline_ms: 1_100,
            },
            ControlPlaneCommand::ExpireHeartbeatLeases {
                expire_at_ms: 2_000,
            },
            ControlPlaneCommand::EstablishLeaseGrantHorizon {
                authority: LeaseHorizonAuthorityBinding::new(7, Some(11)),
                authority_now_ms: 2_100,
                horizon_duration_ms: 30_000,
            },
            ControlPlaneCommand::SetPgActingSet {
                pg_id: PgId::new(3),
                acting_set: vec![NodeId::new(1), NodeId::new(2)],
            },
            ControlPlaneCommand::SetPgActingSetWithMetadataTransfer {
                pg_id: PgId::new(4),
                acting_set: vec![NodeId::new(2)],
                transfer,
            },
            ControlPlaneCommand::FencePgForMetadataTransfer {
                pg_id: PgId::new(3),
            },
            ControlPlaneCommand::SetPgState {
                pg_id: PgId::new(3),
                state: PgState::Backfilling,
            },
            ControlPlaneCommand::CompletePgPeering {
                pg_id: PgId::new(3),
                primary: NodeId::new(1),
                node_incarnation: 12,
                complete_at_ms: 3_000,
            },
            ControlPlaneCommand::CompleteReadyPgPeerings {
                ready_at_ms: 4_000,
                ready: vec![ReadyPgPeeringCompletion {
                    pg_id: PgId::new(3),
                    primary: NodeId::new(1),
                    node_incarnation: 12,
                    active_metadata_proof: proof,
                    active_metadata_proof_epoch: ClusterEpoch::new(13).unwrap(),
                }],
            },
        ]
    }

    fn degenerate_commands() -> Vec<ControlPlaneCommand> {
        let max_proof = PgMetadataProof {
            applied_log_index: u64::MAX,
            applied_log_hash: u64::MAX,
            state_digest: u64::MAX,
        };
        vec![
            ControlPlaneCommand::BootstrapInitialClusterMap {
                nodes: Vec::new(),
                pg_ids: Vec::new(),
            },
            ControlPlaneCommand::RecordNodeHeartbeat {
                heartbeat: NodeHeartbeat {
                    node_id: NodeId::new(u32::MAX),
                    node_incarnation: u64::MAX,
                    endpoint: String::new(),
                    observed_epoch: ClusterEpoch::new(u64::MAX).unwrap(),
                    requested_lease_duration_ms: u64::MAX,
                    cluster_map_history_route_references: Default::default(),
                    pg_observations: Vec::new(),
                },
                heartbeat_at_ms: u64::MAX,
                lease_deadline_ms: u64::MAX,
            },
            ControlPlaneCommand::EstablishLeaseGrantHorizon {
                authority: LeaseHorizonAuthorityBinding::new(u64::MAX, Some(u64::MAX)),
                authority_now_ms: u64::MAX,
                horizon_duration_ms: u64::MAX,
            },
            ControlPlaneCommand::SetPgActingSet {
                pg_id: PgId::new(u32::MAX),
                acting_set: Vec::new(),
            },
            ControlPlaneCommand::SetPgActingSetWithMetadataTransfer {
                pg_id: PgId::new(u32::MAX),
                acting_set: Vec::new(),
                transfer: PgMetadataTransferProof::new_with_imported_metadata_proof(
                    ClusterEpoch::new(u64::MAX).unwrap(),
                    max_proof,
                    max_proof,
                ),
            },
            ControlPlaneCommand::CompletePgPeering {
                pg_id: PgId::new(u32::MAX),
                primary: NodeId::new(u32::MAX),
                node_incarnation: u64::MAX,
                complete_at_ms: u64::MAX,
            },
            ControlPlaneCommand::CompleteReadyPgPeerings {
                ready_at_ms: u64::MAX,
                ready: Vec::new(),
            },
        ]
    }

    fn command_frame(tag: u16, write_body: impl FnOnce(&mut Vec<u8>)) -> Vec<u8> {
        command_frame_with_version(CONTROL_PLANE_COMMAND_VERSION, tag, write_body)
    }

    fn command_frame_with_version(
        version: u16,
        tag: u16,
        write_body: impl FnOnce(&mut Vec<u8>),
    ) -> Vec<u8> {
        let mut encoded = Vec::new();
        encoded.extend_from_slice(CONTROL_PLANE_COMMAND_MAGIC);
        write_u16(&mut encoded, version);
        write_u16(&mut encoded, tag);
        write_body(&mut encoded);
        append_control_plane_command_checksum(&mut encoded);
        encoded
    }

    fn assert_decode_error_contains(encoded: &[u8], expected: &str) {
        assert!(matches!(
            decode_control_plane_command(encoded),
            Err(ControlPlaneError::CommandDecode { message }) if message.contains(expected)
        ));
    }

    fn sample_snapshot() -> ClusterControlSnapshot {
        let mut snapshot = ClusterControlSnapshot::empty();
        for command in sample_snapshot_commands() {
            snapshot = snapshot
                .apply_control_plane_command(command)
                .unwrap()
                .into_snapshot();
        }
        snapshot
    }

    fn sample_snapshot_commands() -> Vec<ControlPlaneCommand> {
        vec![
            ControlPlaneCommand::BootstrapInitialClusterMap {
                nodes: vec![
                    (NodeId::new(1), "/tmp/node-1.sock".to_owned()),
                    (NodeId::new(2), "/tmp/node-2.sock".to_owned()),
                ],
                pg_ids: vec![PgId::new(7)],
            },
            ControlPlaneCommand::RecordNodeHeartbeat {
                heartbeat: NodeHeartbeat {
                    node_id: NodeId::new(1),
                    node_incarnation: 0,
                    endpoint: "/tmp/node-1.sock".to_owned(),
                    observed_epoch: ClusterEpoch::new(ClusterEpoch::INITIAL.get() + 1).unwrap(),
                    requested_lease_duration_ms: 100,
                    cluster_map_history_route_references: Default::default(),
                    pg_observations: Vec::new(),
                },
                heartbeat_at_ms: 1_000,
                lease_deadline_ms: 1_100,
            },
            ControlPlaneCommand::SetNodeMembership {
                node_id: NodeId::new(2),
                membership: NodeMembershipState::Draining,
            },
        ]
    }

    fn follow_up_command() -> ControlPlaneCommand {
        ControlPlaneCommand::MarkNodeAvailability {
            node_id: NodeId::new(1),
            availability: NodeAvailabilityState::Unavailable,
        }
    }

    fn log_id(term: u64, index: u64) -> ControlPlaneLogId {
        ControlPlaneLogId::new(term, index).unwrap()
    }

    fn replay_sample_state_machine() -> ReplicatedControlPlaneStateMachine {
        replay_sample_state_machine_with_log_ids([log_id(1, 1), log_id(1, 2), log_id(1, 3)])
    }

    fn replay_sample_state_machine_with_log_ids(
        log_ids: [ControlPlaneLogId; 3],
    ) -> ReplicatedControlPlaneStateMachine {
        let mut state_machine = ReplicatedControlPlaneStateMachine::empty();
        for (log_id, command) in log_ids.into_iter().zip(sample_snapshot_commands()) {
            state_machine
                .apply_committed_command(log_id, command)
                .unwrap();
        }
        state_machine
    }

    fn replay_sample_directly() -> ClusterControlSnapshot {
        let mut snapshot = ClusterControlSnapshot::empty();
        for command in sample_snapshot_commands() {
            snapshot = snapshot
                .apply_control_plane_command(command)
                .unwrap()
                .into_snapshot();
        }
        snapshot
    }

    fn snapshot_frame_with_version(version: u16, contents: &str) -> Vec<u8> {
        let mut encoded = Vec::new();
        encoded.extend_from_slice(CONTROL_PLANE_SNAPSHOT_MAGIC);
        write_u16(&mut encoded, version);
        write_string(&mut encoded, contents).unwrap();
        append_control_plane_snapshot_checksum(&mut encoded);
        encoded
    }

    #[test]
    fn control_plane_command_codec_round_trips_all_variants() {
        for command in sample_commands() {
            let encoded = encode_control_plane_command(&command).unwrap();
            let decoded = decode_control_plane_command(&encoded).unwrap();
            assert_eq!(decoded, command);
        }
    }

    #[test]
    fn control_plane_command_codec_round_trips_degenerate_values() {
        for command in degenerate_commands() {
            let encoded = encode_control_plane_command(&command).unwrap();
            let decoded = decode_control_plane_command(&encoded).unwrap();
            assert_eq!(decoded, command);
        }
    }

    #[test]
    fn control_plane_command_codec_rejects_incompatible_or_malformed_payloads() {
        assert_decode_error_contains(b"not a command", "truncated");

        let encoded = command_frame_with_version(1, 5, |body| write_u64(body, 1_000));
        assert_decode_error_contains(&encoded, "unsupported control-plane command version 1");

        let mut encoded = encode_control_plane_command(&ControlPlaneCommand::SetPgState {
            pg_id: PgId::new(7),
            state: PgState::Peering,
        })
        .unwrap();
        encoded.truncate(encoded.len() - 1);
        assert_decode_error_contains(&encoded, "checksum mismatch");

        let mut encoded = encode_control_plane_command(&ControlPlaneCommand::SetNodeMembership {
            node_id: NodeId::new(7),
            membership: NodeMembershipState::Active,
        })
        .unwrap();
        encoded.push(0);
        assert_decode_error_contains(&encoded, "checksum mismatch");
    }

    #[test]
    fn control_plane_command_codec_rejects_semantic_decode_errors() {
        let unknown_tag = command_frame(13, |_| {});
        assert_decode_error_contains(&unknown_tag, "unknown control-plane command tag 13");

        let zero_horizon_generation = command_frame(12, |body| {
            write_u64(body, 0);
            write_u8(body, 0);
            write_u64(body, 1_000);
            write_u64(body, 10_000);
        });
        assert_decode_error_contains(
            &zero_horizon_generation,
            "lease horizon clock generation and present Raft term must be nonzero",
        );

        let zero_horizon_raft_term = command_frame(12, |body| {
            write_u64(body, 1);
            write_u8(body, 1);
            write_u64(body, 0);
            write_u64(body, 1_000);
            write_u64(body, 10_000);
        });
        assert_decode_error_contains(
            &zero_horizon_raft_term,
            "lease horizon clock generation and present Raft term must be nonzero",
        );

        let invalid_membership = command_frame(2, |body| {
            write_u32(body, 7);
            write_u8(body, 99);
        });
        assert_decode_error_contains(&invalid_membership, "invalid node membership state code 99");

        let invalid_availability = command_frame(3, |body| {
            write_u32(body, 7);
            write_u8(body, 99);
        });
        assert_decode_error_contains(
            &invalid_availability,
            "invalid node availability state code 99",
        );

        let invalid_pg_state = command_frame(9, |body| {
            write_u32(body, 7);
            write_u8(body, 99);
        });
        assert_decode_error_contains(&invalid_pg_state, "invalid PG state code 99");

        let invalid_pending_presence = command_frame(4, |body| {
            write_minimal_heartbeat_prefix(body);
            write_u32(body, 0);
            write_u32(body, 1);
            write_u32(body, 7);
            write_pg_state(body, PgState::Peering);
            write_pg_metadata_proof(
                body,
                PgMetadataProof {
                    applied_log_index: 1,
                    applied_log_hash: 2,
                    state_digest: 3,
                },
            );
            write_u8(body, 2);
        });
        assert_decode_error_contains(
            &invalid_pending_presence,
            "invalid pending metadata command presence code 2",
        );

        let invalid_history_route_kind = command_frame(4, |body| {
            write_minimal_heartbeat_prefix(body);
            write_u32(body, 1);
            write_u8(body, 99);
            write_u64(body, 1);
            write_u32(body, 7);
        });
        assert_decode_error_contains(
            &invalid_history_route_kind,
            "invalid cluster-map history route reference kind 99",
        );

        let noncanonical_history_routes = command_frame(4, |body| {
            write_minimal_heartbeat_prefix(body);
            write_u32(body, 2);
            for epoch in [2, 1] {
                write_u8(body, 1);
                write_u64(body, epoch);
                write_u32(body, 7);
            }
        });
        assert_decode_error_contains(
            &noncanonical_history_routes,
            "cluster-map history route references are not in canonical order",
        );

        let zero_epoch = command_frame(4, |body| {
            write_u32(body, 7);
            write_u64(body, 0);
            write_string(body, "/tmp/node.sock").unwrap();
            write_u64(body, 0);
        });
        assert_decode_error_contains(&zero_epoch, "heartbeat observed epoch must be nonzero");

        let non_utf8_string = command_frame(1, |body| {
            write_u32(body, 1);
            write_u32(body, 7);
            write_u32(body, 1);
            write_u8(body, 0xff);
        });
        assert_decode_error_contains(&non_utf8_string, "not UTF-8");
    }

    #[test]
    fn control_plane_command_codec_rejects_valid_frame_bit_flip() {
        let command = ControlPlaneCommand::SetNodeMembership {
            node_id: NodeId::new(7),
            membership: NodeMembershipState::Active,
        };
        let mut encoded = encode_control_plane_command(&command).unwrap();
        let node_id_offset = CONTROL_PLANE_COMMAND_MAGIC.len()
            + std::mem::size_of::<u16>()
            + std::mem::size_of::<u16>();
        encoded[node_id_offset] ^= 1;

        assert_decode_error_contains(&encoded, "control-plane command checksum mismatch");
    }

    #[test]
    fn control_plane_command_codec_rejects_oversized_ready_completion_count_before_allocation() {
        let encoded = command_frame(11, |body| {
            write_u64(body, 4_000);
            write_u32(body, 1);
            body.extend_from_slice(&[0; CONTROL_PLANE_COMMAND_READY_PG_MIN_LEN - 4]);
        });

        assert_decode_error_contains(&encoded, "ready PG peering completions count 1 exceeds");
    }

    #[test]
    fn control_plane_snapshot_codec_round_trips_and_continues_replay() {
        let snapshot = sample_snapshot()
            .apply_control_plane_command(ControlPlaneCommand::EstablishLeaseGrantHorizon {
                authority: LeaseHorizonAuthorityBinding::new(4, Some(6)),
                authority_now_ms: 2_000,
                horizon_duration_ms: 30_000,
            })
            .unwrap()
            .into_snapshot();
        let encoded = encode_control_plane_snapshot(&snapshot).unwrap();
        let decoded = decode_control_plane_snapshot(&encoded).unwrap();
        assert_eq!(decoded, snapshot);

        let command = ControlPlaneCommand::MarkNodeAvailability {
            node_id: NodeId::new(1),
            availability: NodeAvailabilityState::Unavailable,
        };
        let expected = snapshot
            .apply_control_plane_command(command.clone())
            .unwrap()
            .into_snapshot();
        let actual = decoded
            .apply_control_plane_command(command)
            .unwrap()
            .into_snapshot();
        assert_eq!(actual, expected);
    }

    #[test]
    fn control_plane_snapshot_codec_rejects_corrupt_or_incompatible_frames() {
        assert!(matches!(
            decode_control_plane_snapshot(b"not a snapshot"),
            Err(ControlPlaneError::SnapshotDecode { message }) if message.contains("truncated")
        ));

        let encoded = snapshot_frame_with_version(2, &format_snapshot(&sample_snapshot()));
        assert!(matches!(
            decode_control_plane_snapshot(&encoded),
            Err(ControlPlaneError::SnapshotDecode { message })
                if message.contains("unsupported control-plane snapshot version 2")
        ));

        let mut encoded = encode_control_plane_snapshot(&sample_snapshot()).unwrap();
        let content_offset = CONTROL_PLANE_SNAPSHOT_MAGIC.len()
            + std::mem::size_of::<u16>()
            + std::mem::size_of::<u32>();
        encoded[content_offset] ^= 1;
        assert!(matches!(
            decode_control_plane_snapshot(&encoded),
            Err(ControlPlaneError::SnapshotDecode { message })
                if message.contains("control-plane snapshot checksum mismatch")
        ));

        let encoded = snapshot_frame_with_version(CONTROL_PLANE_SNAPSHOT_VERSION, "version=99\n");
        assert!(matches!(
            decode_control_plane_snapshot(&encoded),
            Err(ControlPlaneError::Parse { .. })
        ));
    }

    #[test]
    fn replicated_control_plane_state_machine_replay_matches_direct_apply() {
        let state_machine = replay_sample_state_machine();
        assert_eq!(state_machine.snapshot(), &replay_sample_directly());
        assert_eq!(state_machine.last_applied(), Some(log_id(1, 3)));
        assert_eq!(state_machine.snapshot_last_applied(), None);
    }

    #[test]
    fn control_plane_log_id_rejects_zero_term_or_index() {
        assert_eq!(ControlPlaneLogId::new(0, 1), None);
        assert_eq!(ControlPlaneLogId::new(1, 0), None);
        assert_eq!(ControlPlaneLogId::new(1, 1), Some(log_id(1, 1)));
    }

    #[test]
    fn replicated_control_plane_state_machine_builds_read_index_runtime_map() {
        let state_machine = replay_sample_state_machine();
        let read_index = log_id(1, 3);
        let runtime_map = state_machine
            .runtime_map_for_read_index(read_index, 12_345)
            .unwrap();

        assert_eq!(
            runtime_map.freshness_proof(),
            &RuntimeMapFreshnessProof::ReadIndex {
                authority_incarnation: state_machine.snapshot().authority_incarnation(),
                read_index,
                issued_at_ms: 12_345,
            }
        );
        assert_eq!(runtime_map.freshness_proof().read_index(), Some(read_index));
        assert!(runtime_map.freshness_proof().is_serving_authority_read());
    }

    #[test]
    fn replicated_control_plane_state_machine_rejects_unapplied_read_index() {
        let state_machine = ReplicatedControlPlaneStateMachine::empty();
        assert!(matches!(
            state_machine.runtime_map_for_read_index(log_id(1, 1), 12_345),
            Err(ControlPlaneError::ControlPlaneReadIndexNotApplied {
                read_index,
                last_applied: None,
            }) if read_index == log_id(1, 1)
        ));

        let state_machine = replay_sample_state_machine();
        assert!(matches!(
            state_machine.runtime_map_for_read_index(log_id(1, 4), 12_345),
            Err(ControlPlaneError::ControlPlaneReadIndexNotApplied {
                read_index,
                last_applied: Some(last_applied),
            }) if read_index == log_id(1, 4) && last_applied == log_id(1, 3)
        ));
        assert!(matches!(
            state_machine.runtime_map_for_read_index(log_id(2, 3), 12_345),
            Err(ControlPlaneError::ControlPlaneReadIndexNotApplied {
                read_index,
                last_applied: Some(last_applied),
            }) if read_index == log_id(2, 3) && last_applied == log_id(1, 3)
        ));
        assert!(matches!(
            state_machine.runtime_map_for_read_index(log_id(2, 2), 12_345),
            Err(ControlPlaneError::ControlPlaneReadIndexNotApplied {
                read_index,
                last_applied: Some(last_applied),
            }) if read_index == log_id(2, 2) && last_applied == log_id(1, 3)
        ));

        let state_machine =
            replay_sample_state_machine_with_log_ids([log_id(1, 1), log_id(2, 2), log_id(2, 3)]);
        assert!(matches!(
            state_machine.runtime_map_for_read_index(log_id(1, 4), 12_345),
            Err(ControlPlaneError::ControlPlaneReadIndexNotApplied {
                read_index,
                last_applied: Some(last_applied),
            }) if read_index == log_id(1, 4) && last_applied == log_id(2, 3)
        ));
        assert!(matches!(
            state_machine.runtime_map_for_read_index(log_id(1, 3), 12_345),
            Err(ControlPlaneError::ControlPlaneReadIndexNotApplied {
                read_index,
                last_applied: Some(last_applied),
            }) if read_index == log_id(1, 3) && last_applied == log_id(2, 3)
        ));
        assert!(matches!(
            state_machine.runtime_map_for_read_index(log_id(1, 2), 12_345),
            Err(ControlPlaneError::ControlPlaneReadIndexNotApplied {
                read_index,
                last_applied: Some(last_applied),
            }) if read_index == log_id(1, 2) && last_applied == log_id(2, 3)
        ));
    }

    #[test]
    fn replicated_control_plane_state_machine_rejects_non_contiguous_log_before_mutation() {
        let mut state_machine = ReplicatedControlPlaneStateMachine::empty();
        let before = state_machine.clone();

        assert!(matches!(
            state_machine
                .apply_committed_command(log_id(1, 2), sample_snapshot_commands()[0].clone()),
            Err(ControlPlaneError::ControlPlaneLogIndexMismatch {
                expected_index: 1,
                actual_index: 2,
            })
        ));
        assert_eq!(state_machine, before);

        state_machine
            .apply_committed_command(log_id(1, 1), sample_snapshot_commands()[0].clone())
            .unwrap();
        let before = state_machine.clone();
        assert!(matches!(
            state_machine
                .apply_committed_command(log_id(1, 1), sample_snapshot_commands()[1].clone()),
            Err(ControlPlaneError::ControlPlaneLogIndexMismatch {
                expected_index: 2,
                actual_index: 1,
            })
        ));
        assert_eq!(state_machine, before);
    }

    #[test]
    fn replicated_control_plane_state_machine_rejects_term_regression_before_mutation() {
        let mut state_machine = ReplicatedControlPlaneStateMachine::empty();
        state_machine
            .apply_committed_command(log_id(2, 1), sample_snapshot_commands()[0].clone())
            .unwrap();
        let before = state_machine.clone();

        assert!(matches!(
            state_machine
                .apply_committed_command(log_id(1, 2), sample_snapshot_commands()[1].clone()),
            Err(ControlPlaneError::ControlPlaneLogTermRegression {
                previous_term: 2,
                actual_term: 1,
                index: 2,
            })
        ));
        assert_eq!(state_machine, before);
    }

    #[test]
    fn replicated_control_plane_state_machine_rejects_log_index_overflow_before_mutation() {
        let mut state_machine =
            ReplicatedControlPlaneStateMachine::new(sample_snapshot(), Some(log_id(1, u64::MAX)))
                .unwrap();
        let before = state_machine.clone();

        assert!(matches!(
            state_machine.apply_committed_command(log_id(1, 1), follow_up_command()),
            Err(ControlPlaneError::ControlPlaneLogIndexOverflow { index: u64::MAX })
        ));
        assert_eq!(state_machine, before);
    }

    #[test]
    fn replicated_control_plane_state_machine_fatal_invariant_does_not_advance() {
        let mut state_machine = ReplicatedControlPlaneStateMachine {
            snapshot: ClusterControlSnapshot::test_invalid_active_without_metadata_proof_epoch(
                PgId::new(27),
            ),
            last_applied: Some(log_id(1, 1)),
            snapshot_last_applied: None,
        };
        let before = state_machine.clone();

        let error = state_machine
            .apply_committed_command(
                log_id(1, 2),
                ControlPlaneCommand::MarkNodeAvailability {
                    node_id: NodeId::new(1),
                    availability: NodeAvailabilityState::Suspect,
                },
            )
            .unwrap_err();

        assert!(matches!(
            error,
            ControlPlaneError::SnapshotInvariantViolation {
                context: "control-plane command produced invalid snapshot",
                message,
            } if message.contains("has no metadata proof epoch")
        ));
        assert_eq!(state_machine, before);
        assert_eq!(state_machine.last_applied(), Some(log_id(1, 1)));
    }

    #[test]
    fn replicated_control_plane_state_machine_commits_deterministic_command_rejection() {
        let mut state_machine = ReplicatedControlPlaneStateMachine::empty();
        let before = state_machine.snapshot().clone();
        let rejected = state_machine
            .apply_committed_command(
                log_id(1, 1),
                ControlPlaneCommand::MarkNodeAvailability {
                    node_id: NodeId::new(99),
                    availability: NodeAvailabilityState::Unavailable,
                },
            )
            .unwrap();

        assert_eq!(rejected.log_id(), log_id(1, 1));
        assert!(matches!(
            rejected.outcome(),
            CommittedControlPlaneCommandOutcome::Rejected(ControlPlaneError::UnknownNode {
                node_id: 99,
            })
        ));
        assert_eq!(state_machine.snapshot(), &before);
        assert_eq!(state_machine.last_applied(), Some(log_id(1, 1)));

        let applied = state_machine
            .apply_committed_command(log_id(1, 2), sample_snapshot_commands()[0].clone())
            .unwrap();
        assert!(matches!(
            applied.outcome(),
            CommittedControlPlaneCommandOutcome::Applied(_)
        ));
        assert_eq!(state_machine.last_applied(), Some(log_id(1, 2)));
    }

    #[test]
    fn replicated_control_plane_state_machine_snapshots_after_committed_rejection() {
        let mut source = ReplicatedControlPlaneStateMachine::empty();
        source
            .apply_committed_command(log_id(1, 1), sample_snapshot_commands()[0].clone())
            .unwrap();
        source
            .apply_committed_command(
                log_id(1, 2),
                ControlPlaneCommand::MarkNodeAvailability {
                    node_id: NodeId::new(99),
                    availability: NodeAvailabilityState::Unavailable,
                },
            )
            .unwrap();
        assert_eq!(source.last_applied(), Some(log_id(1, 2)));

        let artifact = source.build_snapshot_artifact().unwrap();
        assert_eq!(artifact.last_applied(), Some(log_id(1, 2)));

        let mut installed = ReplicatedControlPlaneStateMachine::empty();
        installed.install_snapshot_artifact(artifact).unwrap();
        assert_eq!(installed.snapshot(), source.snapshot());
        assert_eq!(installed.last_applied(), Some(log_id(1, 2)));
        assert_eq!(installed.snapshot_last_applied(), Some(log_id(1, 2)));

        let command = sample_snapshot_commands()[1].clone();
        let expected = source
            .apply_committed_command(log_id(1, 3), command.clone())
            .unwrap()
            .into_applied_command()
            .expect("post-rejection follow-up should apply")
            .into_snapshot();
        let actual = installed
            .apply_committed_command(log_id(1, 3), command)
            .unwrap()
            .into_applied_command()
            .expect("post-rejection follow-up should apply after snapshot install")
            .into_snapshot();
        assert_eq!(actual, expected);
    }

    #[test]
    fn replicated_control_plane_state_machine_snapshot_install_continues_replay() {
        let mut source = replay_sample_state_machine();
        let artifact = source.build_snapshot_artifact().unwrap();
        assert_eq!(artifact.last_applied(), Some(log_id(1, 3)));
        assert_eq!(source.snapshot_last_applied(), Some(log_id(1, 3)));

        let mut installed = ReplicatedControlPlaneStateMachine::empty();
        installed.install_snapshot_artifact(artifact).unwrap();
        assert_eq!(installed.snapshot(), source.snapshot());
        assert_eq!(installed.last_applied(), Some(log_id(1, 3)));
        assert_eq!(installed.snapshot_last_applied(), Some(log_id(1, 3)));

        let command = follow_up_command();
        let expected = source
            .apply_committed_command(log_id(2, 4), command.clone())
            .unwrap()
            .into_applied_command()
            .expect("follow-up should apply")
            .into_snapshot();
        let actual = installed
            .apply_committed_command(log_id(2, 4), command)
            .unwrap()
            .into_applied_command()
            .expect("follow-up should apply after snapshot install")
            .into_snapshot();
        assert_eq!(actual, expected);
        assert_eq!(installed.last_applied(), Some(log_id(2, 4)));
        assert_eq!(installed.snapshot_last_applied(), Some(log_id(1, 3)));
    }

    #[test]
    fn replicated_control_plane_state_machine_accepts_current_and_forward_snapshot_install() {
        let mut source = replay_sample_state_machine();
        let artifact = source.build_snapshot_artifact().unwrap();

        let mut installed = replay_sample_state_machine();
        installed.install_snapshot_artifact(artifact).unwrap();
        assert_eq!(installed.snapshot(), source.snapshot());
        assert_eq!(installed.last_applied(), Some(log_id(1, 3)));
        assert_eq!(installed.snapshot_last_applied(), Some(log_id(1, 3)));

        source
            .apply_committed_command(log_id(2, 4), follow_up_command())
            .unwrap();
        let artifact = source.build_snapshot_artifact().unwrap();
        installed.install_snapshot_artifact(artifact).unwrap();
        assert_eq!(installed.snapshot(), source.snapshot());
        assert_eq!(installed.last_applied(), Some(log_id(2, 4)));
        assert_eq!(installed.snapshot_last_applied(), Some(log_id(2, 4)));
    }

    #[test]
    fn replicated_control_plane_state_machine_rejects_corrupt_snapshot_install_before_mutation() {
        let mut source = replay_sample_state_machine();
        let mut artifact = source.build_snapshot_artifact().unwrap();
        let content_offset = CONTROL_PLANE_SNAPSHOT_MAGIC.len()
            + std::mem::size_of::<u16>()
            + std::mem::size_of::<u32>();
        artifact.payload[content_offset] ^= 1;

        let mut installed = replay_sample_state_machine();
        let before = installed.clone();
        assert!(matches!(
            installed.install_snapshot_artifact(artifact),
            Err(ControlPlaneError::SnapshotDecode { message })
                if message.contains("control-plane snapshot checksum mismatch")
        ));
        assert_eq!(installed, before);
    }

    #[test]
    fn replicated_control_plane_state_machine_rejects_stale_valid_snapshot_install_before_mutation()
    {
        let snapshot = sample_snapshot();
        let payload = encode_control_plane_snapshot(&snapshot).unwrap();

        let mut current = replay_sample_state_machine();
        current
            .apply_committed_command(log_id(2, 4), follow_up_command())
            .unwrap();
        let before = current.clone();
        assert!(matches!(
            current.install_snapshot_artifact(ControlPlaneSnapshotArtifact::new(
                None,
                payload.clone()
            )),
            Err(ControlPlaneError::ControlPlaneSnapshotMissingLogId { current_index: 4 })
        ));
        assert_eq!(current, before);

        let before = current.clone();
        assert!(matches!(
            current.install_snapshot_artifact(ControlPlaneSnapshotArtifact::new(
                Some(log_id(1, 3)),
                payload.clone(),
            )),
            Err(ControlPlaneError::ControlPlaneSnapshotLogIndexRegression {
                current_index: 4,
                artifact_index: 3,
            })
        ));
        assert_eq!(current, before);

        let mut current =
            ReplicatedControlPlaneStateMachine::new(snapshot.clone(), Some(log_id(2, 3))).unwrap();
        let before = current.clone();
        assert!(matches!(
            current.install_snapshot_artifact(ControlPlaneSnapshotArtifact::new(
                Some(log_id(1, 3)),
                payload.clone(),
            )),
            Err(ControlPlaneError::ControlPlaneSnapshotLogTermMismatch {
                index: 3,
                current_term: 2,
                artifact_term: 1,
            })
        ));
        assert_eq!(current, before);

        let before = current.clone();
        assert!(matches!(
            current.install_snapshot_artifact(ControlPlaneSnapshotArtifact::new(
                Some(log_id(1, 4)),
                payload,
            )),
            Err(ControlPlaneError::ControlPlaneLogTermRegression {
                previous_term: 2,
                actual_term: 1,
                index: 4,
            })
        ));
        assert_eq!(current, before);
    }

    fn write_minimal_heartbeat_prefix(out: &mut Vec<u8>) {
        write_u32(out, 7);
        write_u64(out, 0);
        write_string(out, "/tmp/node.sock").unwrap();
        write_u64(out, 1);
        write_u64(out, 100);
    }
}
