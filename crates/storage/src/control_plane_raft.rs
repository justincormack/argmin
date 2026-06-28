use std::collections::BTreeMap;
use std::io::{self, Cursor};
use std::ops::{Bound, RangeBounds};
use std::sync::{Arc, Mutex, MutexGuard};

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
use openraft::RaftLogReader;
use openraft::RaftSnapshotBuilder;
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

fn control_plane_error_to_io_error(context: &'static str, error: ControlPlaneError) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, format!("{context}: {error}"))
}

fn raft_log_store_error(message: impl Into<String>) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, message.into())
}

#[derive(Debug, Clone, Default)]
pub struct ControlPlaneRaftLogStore {
    inner: Arc<Mutex<ControlPlaneRaftLogStoreInner>>,
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
}

impl ControlPlaneRaftLogStoreInner {
    fn last_log_id(&self) -> Option<LogIdOf<ControlPlaneRaftTypeConfig>> {
        self.entries
            .last_key_value()
            .map(|(_, entry)| entry.log_id)
            .or(self.last_purged_log_id)
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
        self.lock()?.vote = Some(*vote);
        Ok(())
    }

    async fn save_committed(
        &mut self,
        committed: Option<LogIdOf<ControlPlaneRaftTypeConfig>>,
    ) -> Result<(), io::Error> {
        self.lock()?.committed = committed;
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
        let Some(last_log_id) = last_log_id else {
            inner.entries.clear();
            return Ok(());
        };

        if inner
            .last_purged_log_id
            .is_some_and(|purged| last_log_id.index() < purged.index())
        {
            return Err(raft_log_store_error(format!(
                "cannot truncate control-plane OpenRaft log after purged boundary {last_log_id}"
            )));
        }
        if let Some(existing) = inner.entries.get(&last_log_id.index()) {
            if existing.log_id != last_log_id {
                return Err(raft_log_store_error(format!(
                    "cannot truncate control-plane OpenRaft log after mismatched log id {last_log_id}; stored {}",
                    existing.log_id
                )));
            }
        }
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
        let Some(current_last_log_id) = inner.last_log_id() else {
            return Err(raft_log_store_error(format!(
                "cannot purge empty control-plane OpenRaft log to {log_id}"
            )));
        };
        if log_id.index() > current_last_log_id.index() {
            return Err(raft_log_store_error(format!(
                "cannot purge control-plane OpenRaft log to {log_id}; current last log id is {current_last_log_id}"
            )));
        }
        if let Some(entry) = inner.entries.get(&log_id.index()) {
            if entry.log_id != log_id {
                return Err(raft_log_store_error(format!(
                    "cannot purge control-plane OpenRaft log to mismatched log id {log_id}; stored {}",
                    entry.log_id
                )));
            }
        }
        inner.entries.retain(|index, _| *index > log_id.index());
        inner.last_purged_log_id = Some(log_id);
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
            snapshot_id: format!(
                "control-plane-{}",
                last_log_id.map(|log_id| log_id.index()).unwrap_or(0)
            ),
        })
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

    use super::*;
    use futures_util::stream;
    use openraft::errors::{RPCError, ReplicationClosed, StreamingError, Unreachable};
    use openraft::network::{RPCOption, RaftNetworkFactory, RaftNetworkV2};
    use openraft::raft::{
        AppendEntriesRequest, AppendEntriesResponse, SnapshotResponse, VoteRequest, VoteResponse,
    };
    use openraft::type_config::TypeConfigExt;
    use openraft::{AnyError, Config, Membership, Raft};

    use crate::control_plane::NodeAvailabilityState;
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

    fn test_raft_config() -> Arc<Config> {
        Arc::new(
            Config {
                cluster_name: "control-plane-raft-test".to_string(),
                heartbeat_interval: 50,
                election_timeout_min: 150,
                election_timeout_max: 300,
                enable_tick: false,
                enable_heartbeat: false,
                enable_elect: false,
                ..Default::default()
            }
            .validate()
            .unwrap(),
        )
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
    fn control_plane_raft_snapshot_builder_returns_stable_snapshot_view() {
        let mut state_machine = ControlPlaneRaftStateMachine::empty();
        state_machine.apply_entry(blank_entry(2, 7, 1)).unwrap();

        let mut builder = state_machine.create_snapshot_builder().unwrap();
        state_machine.apply_entry(blank_entry(2, 7, 2)).unwrap();

        let snapshot = ControlPlaneRaftTypeConfig::run(builder.build_snapshot()).unwrap();

        assert_eq!(snapshot.meta.last_log_id, Some(raft_log_id(2, 7, 1)));
        assert_eq!(snapshot.meta.snapshot_id, "control-plane-1");
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
    fn control_plane_openraft_single_node_initialize_uses_bootstrap_membership() {
        ControlPlaneRaftTypeConfig::run(async {
            let mut log_store = ControlPlaneRaftLogStore::empty();
            let state_machine = ControlPlaneRaftStateMachine::empty();
            let raft = Raft::<ControlPlaneRaftTypeConfig, ControlPlaneRaftStateMachine>::new(
                1,
                test_raft_config(),
                UnreachableRaftNetworkFactory,
                log_store.clone(),
                state_machine,
            )
            .await
            .unwrap();

            raft.initialize(BTreeMap::from([(1, BasicNode::new("node-1"))]))
                .await
                .unwrap();
            assert!(raft.is_initialized().await.unwrap());

            let bootstrap_log_id = raft_log_id(0, 1, 0);
            let raft_state = raft
                .with_raft_state(|state| *state.membership_state.effective().log_id())
                .await
                .unwrap();
            assert_eq!(raft_state, Some(bootstrap_log_id));

            let entries = RaftLogReader::try_get_log_entries(&mut log_store, 0..1)
                .await
                .unwrap();
            assert_eq!(entries.len(), 1);
            assert_eq!(entries[0].log_id, bootstrap_log_id);
            assert!(matches!(entries[0].payload, EntryPayload::Membership(_)));

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
                .unwrap();
            let log_state = RaftLogStorage::get_log_state(&mut store).await.unwrap();
            assert_eq!(log_state.last_purged_log_id, Some(raft_log_id(3, 1, 2)));
            assert_eq!(log_state.last_log_id, Some(raft_log_id(3, 1, 2)));
        });
    }
}
