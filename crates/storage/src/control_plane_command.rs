use crate::control_plane::{
    ClusterControlSnapshot, ControlPlaneError, NodeAvailabilityState, NodeMembershipState,
};
use crate::types::PgId;
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
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ControlPlaneCommandResponse {
    BootstrapInitialClusterMap,
    SetNodeMembership,
    MarkNodeAvailability,
    SetPgActingSet,
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
