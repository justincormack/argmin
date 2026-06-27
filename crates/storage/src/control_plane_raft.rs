use std::io::Cursor;

use openraft::impls::leader_id_adv::LeaderId;
use openraft::impls::BasicNode;
use openraft::impls::Entry;
use openraft::impls::Vote;
use openraft::type_config::alias::LogIdOf;
use openraft::LogId;
use openraft::RaftTypeConfig;
use placement::NodeId;

use crate::control_plane::ControlPlaneError;
use crate::control_plane_command::{
    ControlPlaneCommand, ControlPlaneCommandResponse, ControlPlaneLogId,
};

pub type ControlPlaneRaftNodeId = u64;
pub type ControlPlaneRaftTerm = u64;
pub type ControlPlaneRaftLeaderId = LeaderId<ControlPlaneRaftTerm, ControlPlaneRaftNodeId>;
pub type ControlPlaneRaftEntry =
    Entry<ControlPlaneRaftLeaderId, ControlPlaneCommand, ControlPlaneRaftNodeId, BasicNode>;

#[derive(Debug)]
pub enum ControlPlaneRaftApplyResponse {
    Applied(ControlPlaneCommandResponse),
    Rejected(ControlPlaneError),
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

#[cfg(test)]
mod tests {
    use super::*;

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
}
