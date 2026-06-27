use crate::control_plane::{
    ClusterControlSnapshot, ControlPlaneError, NodeAvailabilityState, NodeMembershipState,
    PgMetadataProof, PgMetadataTransferProof,
};
use crate::types::{PgId, PgState};
use crate::ClusterEpoch;
use placement::NodeId;

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

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ControlPlaneCommandResponse {
    BootstrapInitialClusterMap,
    SetNodeMembership,
    MarkNodeAvailability,
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
    pub fn response(&self) -> ControlPlaneCommandResponse {
        self.response
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
        authority_now_ms: u64,
    ) -> Result<AppliedControlPlaneCommand, ControlPlaneError>;
}
