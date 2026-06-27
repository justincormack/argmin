use crate::control_plane::{
    format_snapshot, parse_snapshot, ClusterControlSnapshot, ControlPlaneError,
    NodeAvailabilityState, NodeHeartbeat, NodeMembershipState, NodePgHeartbeatObservation,
    PgMetadataProof, PgMetadataTransferProof,
};
use crate::types::{PgId, PgState};
use crate::{ClusterEpoch, PgClusterMapHistoryReferenceSummary};
use placement::NodeId;

const CONTROL_PLANE_COMMAND_MAGIC: &[u8; 8] = b"ARGCPCMD";
const CONTROL_PLANE_COMMAND_VERSION: u16 = 1;
const CONTROL_PLANE_COMMAND_CHECKSUM_LEN: usize = 8;
const CONTROL_PLANE_SNAPSHOT_MAGIC: &[u8; 8] = b"ARGCPSNP";
const CONTROL_PLANE_SNAPSHOT_VERSION: u16 = 1;
const CONTROL_PLANE_SNAPSHOT_CHECKSUM_LEN: usize = 8;
const CONTROL_PLANE_COMMAND_BOOTSTRAP_NODE_MIN_LEN: usize = 8;
const CONTROL_PLANE_COMMAND_ACTING_SET_NODE_MIN_LEN: usize = 4;
const CONTROL_PLANE_COMMAND_PG_MIN_LEN: usize = 4;
const CONTROL_PLANE_COMMAND_HEARTBEAT_OBSERVATION_MIN_LEN: usize = 30;
const CONTROL_PLANE_COMMAND_READY_PG_MIN_LEN: usize = 40;

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

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ReadyPgPeeringCompletion {
    pub pg_id: PgId,
    pub primary: NodeId,
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
                write_pg_metadata_proof(&mut out, completion.active_metadata_proof);
                write_u64(&mut out, completion.active_metadata_proof_epoch.get());
            }
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
                    active_metadata_proof: read_pg_metadata_proof(&mut reader)?,
                    active_metadata_proof_epoch: read_cluster_epoch(
                        &mut reader,
                        "ready PG peering proof epoch",
                    )?,
                });
            }
            ControlPlaneCommand::CompleteReadyPgPeerings { ready_at_ms, ready }
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
    pub fn new(
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
    write_option_cluster_epoch(
        out,
        heartbeat
            .cluster_map_history_reference_summary
            .oldest_live_placement_epoch,
    );
    write_option_cluster_epoch(
        out,
        heartbeat
            .cluster_map_history_reference_summary
            .oldest_durable_backfill_epoch,
    );
    write_u32(
        out,
        len_as_u32(heartbeat.pg_observations.len(), "PG observations")?,
    );
    for observation in &heartbeat.pg_observations {
        write_u32(out, observation.pg_id.get());
        write_pg_state(out, observation.state);
        write_pg_metadata_proof(out, observation.metadata_proof);
        write_u8(out, u8::from(observation.has_pending_metadata_command));
    }
    Ok(())
}

fn read_node_heartbeat(reader: &mut PayloadReader<'_>) -> Result<NodeHeartbeat, ControlPlaneError> {
    let node_id = NodeId::new(reader.read_u32()?);
    let node_incarnation = reader.read_u64()?;
    let endpoint = reader.read_string()?.to_owned();
    let observed_epoch = read_cluster_epoch(reader, "heartbeat observed epoch")?;
    let requested_lease_duration_ms = reader.read_u64()?;
    let oldest_live_placement_epoch =
        read_option_cluster_epoch(reader, "heartbeat oldest live placement epoch")?;
    let oldest_durable_backfill_epoch =
        read_option_cluster_epoch(reader, "heartbeat oldest durable backfill epoch")?;
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
            has_pending_metadata_command: reader.read_bool()?,
        });
    }
    Ok(NodeHeartbeat {
        node_id,
        node_incarnation,
        endpoint,
        observed_epoch,
        requested_lease_duration_ms,
        cluster_map_history_reference_summary: PgClusterMapHistoryReferenceSummary {
            oldest_live_placement_epoch,
            oldest_durable_backfill_epoch,
        },
        pg_observations,
    })
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

fn write_option_cluster_epoch(out: &mut Vec<u8>, value: Option<ClusterEpoch>) {
    match value {
        Some(value) => {
            write_u8(out, 1);
            write_u64(out, value.get());
        }
        None => write_u8(out, 0),
    }
}

fn read_option_cluster_epoch(
    reader: &mut PayloadReader<'_>,
    field: &'static str,
) -> Result<Option<ClusterEpoch>, ControlPlaneError> {
    reader
        .read_option_u64()?
        .map(|epoch| {
            ClusterEpoch::new(epoch)
                .ok_or_else(|| command_protocol_error(format!("{field} must be nonzero")))
        })
        .transpose()
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

    fn read_bool(&mut self) -> Result<bool, ControlPlaneError> {
        match self.read_u8()? {
            0 => Ok(false),
            1 => Ok(true),
            value => Err(command_protocol_error(format!(
                "invalid boolean value {value}"
            ))),
        }
    }

    fn read_option_u64(&mut self) -> Result<Option<u64>, ControlPlaneError> {
        match self.read_u8()? {
            0 => Ok(None),
            1 => Ok(Some(self.read_u64()?)),
            value => Err(command_protocol_error(format!(
                "invalid optional u64 tag {value}"
            ))),
        }
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
        let proof = PgMetadataProof::new(7, 8, 9);
        let transfer = PgMetadataTransferProof::new_with_imported_metadata_proof(
            ClusterEpoch::new(11).unwrap(),
            PgMetadataProof::new(1, 2, 3),
            PgMetadataProof::new(4, 5, 6),
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
                    cluster_map_history_reference_summary: PgClusterMapHistoryReferenceSummary {
                        oldest_live_placement_epoch: Some(ClusterEpoch::new(12).unwrap()),
                        oldest_durable_backfill_epoch: Some(ClusterEpoch::new(10).unwrap()),
                    },
                    pg_observations: vec![NodePgHeartbeatObservation {
                        pg_id: PgId::new(3),
                        state: PgState::Peering,
                        metadata_proof: proof,
                        has_pending_metadata_command: true,
                    }],
                },
                heartbeat_at_ms: 1_000,
                lease_deadline_ms: 1_100,
            },
            ControlPlaneCommand::ExpireHeartbeatLeases {
                expire_at_ms: 2_000,
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
                    active_metadata_proof: proof,
                    active_metadata_proof_epoch: ClusterEpoch::new(13).unwrap(),
                }],
            },
        ]
    }

    fn degenerate_commands() -> Vec<ControlPlaneCommand> {
        let max_proof = PgMetadataProof::new(u64::MAX, u64::MAX, u64::MAX);
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
                    cluster_map_history_reference_summary:
                        PgClusterMapHistoryReferenceSummary::default(),
                    pg_observations: Vec::new(),
                },
                heartbeat_at_ms: u64::MAX,
                lease_deadline_ms: u64::MAX,
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
        let commands = vec![
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
                    cluster_map_history_reference_summary:
                        PgClusterMapHistoryReferenceSummary::default(),
                    pg_observations: Vec::new(),
                },
                heartbeat_at_ms: 1_000,
                lease_deadline_ms: 1_100,
            },
            ControlPlaneCommand::SetNodeMembership {
                node_id: NodeId::new(2),
                membership: NodeMembershipState::Draining,
            },
        ];
        for command in commands {
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

        let encoded = command_frame_with_version(2, 5, |body| write_u64(body, 1_000));
        assert_decode_error_contains(&encoded, "unsupported control-plane command version 2");

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
        let unknown_tag = command_frame(12, |_| {});
        assert_decode_error_contains(&unknown_tag, "unknown control-plane command tag 12");

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

        let invalid_bool = command_frame(4, |body| {
            write_minimal_heartbeat_prefix(body);
            write_u8(body, 0);
            write_u8(body, 0);
            write_u32(body, 1);
            write_u32(body, 7);
            write_pg_state(body, PgState::Peering);
            write_pg_metadata_proof(body, PgMetadataProof::new(1, 2, 3));
            write_u8(body, 2);
        });
        assert_decode_error_contains(&invalid_bool, "invalid boolean value 2");

        let invalid_option = command_frame(4, |body| {
            write_minimal_heartbeat_prefix(body);
            write_u8(body, 2);
        });
        assert_decode_error_contains(&invalid_option, "invalid optional u64 tag 2");

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
        let snapshot = sample_snapshot();
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

    fn write_minimal_heartbeat_prefix(out: &mut Vec<u8>) {
        write_u32(out, 7);
        write_u64(out, 0);
        write_string(out, "/tmp/node.sock").unwrap();
        write_u64(out, 1);
        write_u64(out, 100);
    }
}
