use std::io::Cursor;

use openraft::impls::leader_id_adv::LeaderId;
use openraft::impls::BasicNode;
use openraft::impls::Entry;
use openraft::impls::Vote;
use openraft::storage::Snapshot;
use openraft::storage::SnapshotMeta;
use openraft::type_config::alias::{LogIdOf, SnapshotMetaOf, SnapshotOf, StoredMembershipOf};
use openraft::EntryPayload;
use openraft::LogId;
use openraft::RaftTypeConfig;
use openraft::StoredMembership;
use placement::NodeId;

use crate::control_plane::ControlPlaneError;
use crate::control_plane_command::{
    ControlPlaneCommand, ControlPlaneCommandResponse, ControlPlaneLogId,
    ControlPlaneSnapshotArtifact, ReplicatedControlPlaneStateMachine,
};

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

#[derive(Debug, Clone)]
pub struct ControlPlaneRaftStateMachine {
    inner: ReplicatedControlPlaneStateMachine,
    last_applied: Option<LogIdOf<ControlPlaneRaftTypeConfig>>,
    last_membership: StoredMembershipOf<ControlPlaneRaftTypeConfig>,
    current_snapshot: Option<SnapshotOf<ControlPlaneRaftTypeConfig>>,
}

impl ControlPlaneRaftStateMachine {
    #[must_use]
    pub fn empty() -> Self {
        Self::new(
            ReplicatedControlPlaneStateMachine::empty(),
            None,
            StoredMembership::default(),
        )
    }

    #[must_use]
    pub fn new(
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

    pub fn apply_entry(
        &mut self,
        entry: ControlPlaneRaftEntry,
    ) -> Result<ControlPlaneRaftApplyResponse, ControlPlaneError> {
        let raft_log_id = entry.log_id;
        let control_plane_log_id =
            control_plane_log_id_from_raft(raft_log_id).ok_or_else(|| {
                ControlPlaneError::CommandDecode {
                    message: format!(
                        "invalid OpenRaft log id for control-plane entry: {raft_log_id}"
                    ),
                }
            })?;
        match entry.payload {
            EntryPayload::Blank => {
                self.inner.apply_committed_noop(control_plane_log_id)?;
                self.last_applied = Some(raft_log_id);
                Ok(ControlPlaneRaftApplyResponse::Blank)
            }
            EntryPayload::Membership(membership) => {
                self.inner.apply_committed_noop(control_plane_log_id)?;
                self.last_membership = StoredMembership::new(Some(raft_log_id), membership);
                self.last_applied = Some(raft_log_id);
                Ok(ControlPlaneRaftApplyResponse::Membership)
            }
            EntryPayload::Normal(command) => {
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
            None => None,
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
            snapshot_id: format!(
                "control-plane-{}",
                artifact
                    .last_applied()
                    .map(|log_id| log_id.index())
                    .unwrap_or(0)
            ),
        })
    }

    fn validate_snapshot_meta(
        &self,
        meta: &SnapshotMetaOf<ControlPlaneRaftTypeConfig>,
    ) -> Result<Option<ControlPlaneLogId>, ControlPlaneError> {
        let snapshot_log_id = match meta.last_log_id {
            Some(log_id) => Some(Self::validate_snapshot_log_id_shape("last_log_id", log_id)?),
            None => None,
        };
        self.validate_snapshot_install_position(meta.last_log_id)?;
        self.validate_snapshot_membership_position(meta.last_log_id, &meta.last_membership)?;
        Ok(snapshot_log_id)
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
        &self,
        snapshot_last_log_id: Option<LogIdOf<ControlPlaneRaftTypeConfig>>,
        membership: &StoredMembershipOf<ControlPlaneRaftTypeConfig>,
    ) -> Result<(), ControlPlaneError> {
        let Some(membership_log_id) = membership.log_id().as_ref().copied() else {
            return Ok(());
        };
        Self::validate_snapshot_log_id_shape("last_membership.log_id", membership_log_id)?;
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

#[cfg(test)]
mod tests {
    use std::collections::BTreeSet;

    use super::*;
    use openraft::Membership;

    use crate::control_plane::NodeAvailabilityState;
    use crate::types::PgId;

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
    fn control_plane_raft_state_machine_builds_and_installs_snapshot() {
        let mut source = ControlPlaneRaftStateMachine::empty();
        source.apply_entry(blank_entry(2, 7, 1)).unwrap();
        let snapshot = source.build_snapshot().unwrap();

        assert_eq!(snapshot.meta.last_log_id, Some(raft_log_id(2, 7, 1)));
        assert_eq!(snapshot.meta.snapshot_id, "control-plane-1");

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
}
