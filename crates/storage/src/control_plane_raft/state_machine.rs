use super::*;

pub(super) type ControlPlaneRaftSnapshotCache = Arc<Mutex<Option<ControlPlaneRaftSnapshot>>>;

#[cfg(test)]
#[derive(Debug, Default)]
pub(super) struct ControlPlaneRaftStateMachineBlockingHook {
    state: Mutex<ControlPlaneRaftStateMachineBlockingHookState>,
    condition: std::sync::Condvar,
}

#[cfg(test)]
#[derive(Debug, Default)]
struct ControlPlaneRaftStateMachineBlockingHookState {
    entered: bool,
    released: bool,
}

#[cfg(test)]
impl ControlPlaneRaftStateMachineBlockingHook {
    pub(super) fn block(&self) {
        let mut state = self
            .state
            .lock()
            .expect("state-machine blocking hook should not be poisoned");
        state.entered = true;
        self.condition.notify_all();
        while !state.released {
            state = self
                .condition
                .wait(state)
                .expect("state-machine blocking hook should not be poisoned");
        }
    }

    pub(super) fn wait_until_entered(&self, timeout: Duration) {
        let state = self
            .state
            .lock()
            .expect("state-machine blocking hook should not be poisoned");
        let (state, wait) = self
            .condition
            .wait_timeout_while(state, timeout, |state| !state.entered)
            .expect("state-machine blocking hook should not be poisoned");
        assert!(state.entered, "state-machine operation did not enter hook");
        assert!(!wait.timed_out(), "state-machine hook wait timed out");
    }

    pub(super) fn entered(&self) -> bool {
        self.state
            .lock()
            .expect("state-machine blocking hook should not be poisoned")
            .entered
    }

    pub(super) fn release(&self) {
        let mut state = self
            .state
            .lock()
            .expect("state-machine blocking hook should not be poisoned");
        state.released = true;
        self.condition.notify_all();
    }
}

#[cfg(test)]
#[derive(Debug, Clone, Default)]
pub(super) struct ControlPlaneRaftStateMachineTestHooks {
    pub(super) apply: Option<Arc<ControlPlaneRaftStateMachineBlockingHook>>,
    pub(super) snapshot_build: Option<Arc<ControlPlaneRaftStateMachineBlockingHook>>,
    pub(super) snapshot_install: Option<Arc<ControlPlaneRaftStateMachineBlockingHook>>,
    pub(super) retire_generation: Option<Arc<ControlPlaneRaftStateMachineBlockingHook>>,
}

fn lock_control_plane_raft_snapshot_cache(
    cache: &ControlPlaneRaftSnapshotCache,
) -> MutexGuard<'_, Option<ControlPlaneRaftSnapshot>> {
    cache
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
}

pub(super) struct ControlPlaneRaftSnapshotPublication {
    outcome: Result<(), ControlPlaneError>,
    retired: Option<ControlPlaneRaftSnapshot>,
}

impl ControlPlaneRaftSnapshotPublication {
    pub(super) fn finish_on_current_thread(self) -> Result<(), ControlPlaneError> {
        let Self { outcome, retired } = self;
        drop(retired);
        outcome
    }
}

pub(super) fn publish_control_plane_raft_snapshot(
    cache: &ControlPlaneRaftSnapshotCache,
    snapshot: ControlPlaneRaftSnapshot,
) -> ControlPlaneRaftSnapshotPublication {
    let mut current = lock_control_plane_raft_snapshot_cache(cache);
    let should_publish = match current.as_ref() {
        None => true,
        Some(current) => match (current.meta.last_log_id, snapshot.meta.last_log_id) {
            (None, _) => true,
            (Some(_), None) => false,
            (Some(current_log_id), Some(candidate_log_id)) => {
                if candidate_log_id.index() == current_log_id.index()
                    && candidate_log_id != current_log_id
                {
                    return ControlPlaneRaftSnapshotPublication {
                        outcome: Err(ControlPlaneError::SnapshotDecode {
                            message: format!(
                                "OpenRaft snapshot publication log id {candidate_log_id} conflicts with current snapshot {current_log_id} at the same index"
                            ),
                        }),
                        retired: Some(snapshot),
                    };
                }
                candidate_log_id.index() > current_log_id.index()
                    || candidate_log_id == current_log_id
            }
        },
    };
    let retired = if should_publish {
        current.replace(snapshot)
    } else {
        Some(snapshot)
    };
    ControlPlaneRaftSnapshotPublication {
        outcome: Ok(()),
        retired,
    }
}

#[derive(Debug)]
struct ControlPlaneRaftRetiredGeneration {
    inner: ReplicatedControlPlaneStateMachine,
    last_membership: Arc<StoredMembershipOf<ControlPlaneRaftTypeConfig>>,
    cached_snapshot: Option<ControlPlaneRaftSnapshot>,
    #[cfg(test)]
    hook: Option<Arc<ControlPlaneRaftStateMachineBlockingHook>>,
}

impl Drop for ControlPlaneRaftRetiredGeneration {
    fn drop(&mut self) {
        let _ = (&self.inner, &self.last_membership, &self.cached_snapshot);
        #[cfg(test)]
        if let Some(hook) = &self.hook {
            hook.block();
        }
    }
}

impl ControlPlaneRaftRetiredGeneration {
    async fn retire(self, context: &'static str) -> Result<(), io::Error> {
        tokio::task::spawn_blocking(move || drop(self))
            .await
            .map_err(|error| {
                io::Error::other(format!("{context} retirement worker failed: {error}"))
            })
    }
}

#[derive(Debug, Clone)]
struct ControlPlaneRaftSnapshotBuildInput {
    inner: ReplicatedControlPlaneStateMachine,
    last_applied: Option<LogIdOf<ControlPlaneRaftTypeConfig>>,
    last_membership: Arc<StoredMembershipOf<ControlPlaneRaftTypeConfig>>,
    #[cfg(test)]
    hook: Option<Arc<ControlPlaneRaftStateMachineBlockingHook>>,
}

impl ControlPlaneRaftSnapshotBuildInput {
    fn build(mut self) -> Result<ControlPlaneRaftSnapshot, ControlPlaneError> {
        #[cfg(test)]
        if let Some(hook) = &self.hook {
            hook.block();
        }
        let artifact = self.inner.build_snapshot_artifact()?;
        let meta = ControlPlaneRaftStateMachine::snapshot_meta_for_artifact_parts(
            &artifact,
            self.last_applied,
            self.last_membership.as_ref(),
        )?;
        Ok(Snapshot {
            meta,
            snapshot: ControlPlaneRaftSnapshotData::new(artifact.into_payload()),
        })
    }
}

#[derive(Debug, Clone)]
enum ControlPlaneRaftSnapshotBuildWork {
    Ready(ControlPlaneRaftSnapshot),
    Captured(ControlPlaneRaftSnapshotBuildInput),
}

#[derive(Debug, Clone)]
pub struct ControlPlaneRaftSnapshotBuilder {
    work: Result<ControlPlaneRaftSnapshotBuildWork, String>,
    cache: Option<ControlPlaneRaftSnapshotCache>,
}

impl ControlPlaneRaftSnapshotBuilder {
    #[must_use]
    pub fn new(snapshot: ControlPlaneRaftSnapshot) -> Self {
        Self {
            work: Ok(ControlPlaneRaftSnapshotBuildWork::Ready(snapshot)),
            cache: None,
        }
    }

    fn captured(
        input: ControlPlaneRaftSnapshotBuildInput,
        cache: ControlPlaneRaftSnapshotCache,
    ) -> Self {
        Self {
            work: Ok(ControlPlaneRaftSnapshotBuildWork::Captured(input)),
            cache: Some(cache),
        }
    }

    #[must_use]
    pub fn from_error(error: ControlPlaneError) -> Self {
        Self {
            work: Err(error.to_string()),
            cache: None,
        }
    }
}

impl RaftSnapshotBuilder<ControlPlaneRaftTypeConfig> for ControlPlaneRaftSnapshotBuilder {
    type SnapshotData = ControlPlaneRaftSnapshotData;

    async fn build_snapshot(&mut self) -> Result<ControlPlaneRaftSnapshot, io::Error> {
        let work = self
            .work
            .clone()
            .map_err(|message| io::Error::new(io::ErrorKind::InvalidData, message))?;
        let snapshot = match work {
            ControlPlaneRaftSnapshotBuildWork::Ready(snapshot) => snapshot,
            ControlPlaneRaftSnapshotBuildWork::Captured(input) => {
                let cache = self.cache.clone();
                tokio::task::spawn_blocking(move || {
                    let snapshot = input.build()?;
                    if let Some(cache) = cache {
                        publish_control_plane_raft_snapshot(&cache, snapshot.clone())
                            .finish_on_current_thread()?;
                    }
                    Ok::<_, ControlPlaneError>(snapshot)
                })
                .await
                .map_err(|error| {
                    io::Error::other(format!(
                        "control-plane OpenRaft snapshot worker failed: {error}"
                    ))
                })?
                .map_err(|error| {
                    control_plane_error_to_io_error("OpenRaft snapshot build", error)
                })?
            }
        };
        Ok(snapshot)
    }
}

#[derive(Debug)]
pub struct ControlPlaneRaftStateMachine {
    inner: ReplicatedControlPlaneStateMachine,
    last_applied: Option<LogIdOf<ControlPlaneRaftTypeConfig>>,
    last_membership: Arc<StoredMembershipOf<ControlPlaneRaftTypeConfig>>,
    pub(super) current_snapshot: ControlPlaneRaftSnapshotCache,
    #[cfg(test)]
    test_hooks: ControlPlaneRaftStateMachineTestHooks,
}

impl Clone for ControlPlaneRaftStateMachine {
    fn clone(&self) -> Self {
        Self {
            inner: self.inner.clone(),
            last_applied: self.last_applied,
            last_membership: Arc::clone(&self.last_membership),
            current_snapshot: Arc::new(Mutex::new(self.current_snapshot())),
            #[cfg(test)]
            test_hooks: self.test_hooks.clone(),
        }
    }
}

#[derive(Debug, Clone)]
pub(super) struct ControlPlaneRaftStateMachineRestartArtifact {
    pub(super) inner: ReplicatedControlPlaneStateMachine,
    pub(super) last_applied: Option<LogIdOf<ControlPlaneRaftTypeConfig>>,
    pub(super) last_membership: StoredMembershipOf<ControlPlaneRaftTypeConfig>,
    pub(super) current_snapshot: Option<ControlPlaneRaftSnapshot>,
}

impl ControlPlaneRaftStateMachineRestartArtifact {
    pub(super) fn refresh_cached_snapshot(mut self) -> Result<Self, ControlPlaneError> {
        if self.current_snapshot.is_none() {
            return Ok(self);
        }

        let snapshot_artifact = self.inner.build_snapshot_artifact()?;
        let meta = ControlPlaneRaftStateMachine::snapshot_meta_for_artifact_parts(
            &snapshot_artifact,
            self.last_applied,
            &self.last_membership,
        )?;
        self.current_snapshot = Some(Snapshot {
            meta,
            snapshot: ControlPlaneRaftSnapshotData::new(snapshot_artifact.into_payload()),
        });
        Ok(self)
    }

    pub(super) async fn refresh_cached_snapshot_async(self) -> Result<Self, ControlPlaneError> {
        tokio::task::spawn_blocking(move || self.refresh_cached_snapshot())
            .await
            .map_err(|error| {
                ControlPlaneError::io(
                    "refresh control-plane OpenRaft cached restart snapshot",
                    io::Error::other(format!(
                        "control-plane OpenRaft cached restart snapshot worker failed: {error}"
                    )),
                )
            })?
    }
}

impl ControlPlaneRaftStateMachine {
    #[must_use]
    pub fn empty() -> Self {
        Self::from_parts_unchecked(
            ReplicatedControlPlaneStateMachine::empty(),
            None,
            StoredMembership::default(),
        )
    }

    pub fn new(
        inner: ReplicatedControlPlaneStateMachine,
        last_applied: Option<LogIdOf<ControlPlaneRaftTypeConfig>>,
        last_membership: StoredMembershipOf<ControlPlaneRaftTypeConfig>,
    ) -> Result<Self, ControlPlaneError> {
        Self::validate_restart_log_id_consistency(&inner, last_applied)?;
        Self::validate_snapshot_membership_position(last_applied, &last_membership)?;
        Ok(Self::from_parts_unchecked(
            inner,
            last_applied,
            last_membership,
        ))
    }

    #[must_use]
    pub(super) fn export_restart_artifact(&self) -> ControlPlaneRaftStateMachineRestartArtifact {
        let current_snapshot = lock_control_plane_raft_snapshot_cache(&self.current_snapshot)
            .as_ref()
            .filter(|snapshot| {
                Self::snapshot_at_or_before(snapshot.meta.last_log_id, self.last_applied)
            })
            .cloned();
        ControlPlaneRaftStateMachineRestartArtifact {
            inner: self.inner.clone(),
            last_applied: self.last_applied,
            last_membership: self.last_membership.as_ref().clone(),
            current_snapshot,
        }
    }

    pub(super) fn from_restart_artifact(
        artifact: ControlPlaneRaftStateMachineRestartArtifact,
    ) -> Result<Self, ControlPlaneError> {
        let state_machine = Self::new(
            artifact.inner,
            artifact.last_applied,
            artifact.last_membership,
        )?;
        if let Some(snapshot) = artifact.current_snapshot {
            state_machine.validate_cached_snapshot(&snapshot)?;
            publish_control_plane_raft_snapshot(&state_machine.current_snapshot, snapshot)
                .finish_on_current_thread()?;
        }
        Ok(state_machine)
    }

    fn from_parts_unchecked(
        inner: ReplicatedControlPlaneStateMachine,
        last_applied: Option<LogIdOf<ControlPlaneRaftTypeConfig>>,
        last_membership: StoredMembershipOf<ControlPlaneRaftTypeConfig>,
    ) -> Self {
        Self {
            inner,
            last_applied,
            last_membership: Arc::new(last_membership),
            current_snapshot: Arc::new(Mutex::new(None)),
            #[cfg(test)]
            test_hooks: ControlPlaneRaftStateMachineTestHooks::default(),
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
        self.last_membership.as_ref()
    }

    #[must_use]
    pub fn current_snapshot(&self) -> Option<ControlPlaneRaftSnapshot> {
        lock_control_plane_raft_snapshot_cache(&self.current_snapshot).clone()
    }

    #[cfg(test)]
    pub(super) fn set_test_hooks(&mut self, hooks: ControlPlaneRaftStateMachineTestHooks) {
        self.test_hooks = hooks;
    }

    fn capture_apply_candidate(&self) -> Self {
        Self {
            inner: self.inner.clone(),
            last_applied: self.last_applied,
            last_membership: Arc::clone(&self.last_membership),
            current_snapshot: Arc::new(Mutex::new(None)),
            #[cfg(test)]
            test_hooks: self.test_hooks.clone(),
        }
    }

    fn validate_cached_snapshot(
        &self,
        snapshot: &ControlPlaneRaftSnapshot,
    ) -> Result<(), ControlPlaneError> {
        let control_plane_snapshot_log_id = match snapshot.meta.last_log_id {
            Some(log_id) if is_openraft_bootstrap_log_id(log_id) => None,
            Some(log_id) => Some(Self::validate_snapshot_log_id_shape(
                "cached snapshot last_log_id",
                log_id,
            )?),
            None => None,
        };
        Self::validate_snapshot_membership_position(
            snapshot.meta.last_log_id,
            &snapshot.meta.last_membership,
        )?;
        let expected_snapshot_id = Self::snapshot_id_for_log_id(snapshot.meta.last_log_id);
        if snapshot.meta.snapshot_id != expected_snapshot_id {
            return Err(ControlPlaneError::CommandDecode {
                message: format!(
                    "cached OpenRaft snapshot id {} does not match expected {} for last_log_id {:?}",
                    snapshot.meta.snapshot_id, expected_snapshot_id, snapshot.meta.last_log_id
                ),
            });
        }
        if !Self::snapshot_at_or_before(snapshot.meta.last_log_id, self.last_applied) {
            return Err(ControlPlaneError::CommandDecode {
                message: format!(
                    "cached OpenRaft snapshot last_log_id {:?} is after state-machine applied log id {:?}",
                    snapshot.meta.last_log_id, self.last_applied
                ),
            });
        }

        let mut snapshot_inner = ReplicatedControlPlaneStateMachine::empty();
        snapshot_inner.install_snapshot_artifact(ControlPlaneSnapshotArtifact::new(
            control_plane_snapshot_log_id,
            snapshot.snapshot.get_ref().clone(),
        ))?;
        if snapshot_inner.last_applied() != control_plane_snapshot_log_id {
            return Err(ControlPlaneError::CommandDecode {
                message: format!(
                    "cached OpenRaft snapshot payload last-applied {:?} does not match snapshot metadata {:?}",
                    snapshot_inner.last_applied(),
                    control_plane_snapshot_log_id
                ),
            });
        }
        if snapshot.meta.last_log_id == self.last_applied {
            if snapshot.meta.last_membership.log_id() != self.last_membership.log_id()
                || snapshot.meta.last_membership.membership() != self.last_membership.membership()
            {
                return Err(ControlPlaneError::CommandDecode {
                    message:
                        "cached OpenRaft snapshot membership does not match state-machine membership"
                            .to_string(),
                });
            }
            if snapshot_inner.snapshot() != self.inner.snapshot()
                || snapshot_inner.last_applied() != self.inner.last_applied()
            {
                return Err(ControlPlaneError::CommandDecode {
                    message:
                        "cached OpenRaft snapshot payload does not match state-machine restart payload"
                            .to_string(),
                });
            }
        }
        Ok(())
    }

    fn snapshot_at_or_before(
        snapshot_log_id: Option<LogIdOf<ControlPlaneRaftTypeConfig>>,
        last_applied: Option<LogIdOf<ControlPlaneRaftTypeConfig>>,
    ) -> bool {
        match (snapshot_log_id, last_applied) {
            (None, _) => true,
            (Some(_), None) => false,
            (Some(snapshot_log_id), Some(last_applied)) => {
                snapshot_log_id.index() < last_applied.index() || snapshot_log_id == last_applied
            }
        }
    }

    #[must_use]
    pub fn applied_state(
        &self,
    ) -> (
        Option<LogIdOf<ControlPlaneRaftTypeConfig>>,
        StoredMembershipOf<ControlPlaneRaftTypeConfig>,
    ) {
        (self.last_applied, self.last_membership.as_ref().clone())
    }

    pub fn runtime_map_for_applied_read_index(
        &self,
        read_index: LogIdOf<ControlPlaneRaftTypeConfig>,
        issued_at_ms: u64,
    ) -> Result<ClusterRuntimeMapSnapshot, ControlPlaneError> {
        let control_plane_read_index = control_plane_log_id_from_raft(read_index).ok_or_else(|| {
            ControlPlaneError::CommandDecode {
                message: format!(
                    "invalid OpenRaft read-index log id for control-plane runtime map proof: {read_index}"
                ),
            }
        })?;
        if self.last_applied != Some(read_index) {
            if let Some(last_applied) = self.last_applied {
                if last_applied.committed_leader_id().term == read_index.committed_leader_id().term
                    && last_applied.index() == read_index.index()
                {
                    return Err(ControlPlaneError::CommandDecode {
                        message: format!(
                            "OpenRaft read-index log id {read_index} does not match applied log id {last_applied}"
                        ),
                    });
                }
            }
            return Err(ControlPlaneError::ControlPlaneReadIndexNotApplied {
                read_index: control_plane_read_index,
                last_applied: self.inner.last_applied(),
            });
        }
        self.inner
            .runtime_map_for_read_index(control_plane_read_index, issued_at_ms)
    }

    pub fn runtime_map_for_current_applied_read_index(
        &self,
        issued_at_ms: u64,
    ) -> Result<ClusterRuntimeMapSnapshot, ControlPlaneError> {
        let last_applied = self
            .last_applied
            .ok_or_else(|| ControlPlaneError::CommandDecode {
                message: "cannot build OpenRaft read-index runtime map before any log is applied"
                    .to_string(),
            })?;
        self.runtime_map_for_applied_read_index(last_applied, issued_at_ms)
    }

    pub fn apply_entry(
        &mut self,
        entry: ControlPlaneRaftEntry,
    ) -> Result<ControlPlaneRaftApplyResponse, ControlPlaneError> {
        let raft_log_id = entry.log_id;
        self.validate_apply_position(raft_log_id)?;
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
                self.last_membership =
                    Arc::new(StoredMembership::new(Some(raft_log_id), membership));
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

    fn validate_apply_position(
        &self,
        raft_log_id: LogIdOf<ControlPlaneRaftTypeConfig>,
    ) -> Result<(), ControlPlaneError> {
        let Some(last_applied) = self.last_applied else {
            if is_openraft_bootstrap_log_id(raft_log_id) {
                return Ok(());
            }
            if raft_log_id.index() == 0 {
                return Err(ControlPlaneError::CommandDecode {
                    message: format!(
                        "invalid OpenRaft log id for first control-plane entry: {raft_log_id}"
                    ),
                });
            }
            if raft_log_id.index() != 1 {
                return Err(ControlPlaneError::ControlPlaneLogIndexMismatch {
                    expected_index: 1,
                    actual_index: raft_log_id.index(),
                });
            }
            return Ok(());
        };

        let expected_index = last_applied.index().checked_add(1).ok_or(
            ControlPlaneError::ControlPlaneLogIndexOverflow {
                index: last_applied.index(),
            },
        )?;
        if raft_log_id.index() != expected_index {
            return Err(ControlPlaneError::ControlPlaneLogIndexMismatch {
                expected_index,
                actual_index: raft_log_id.index(),
            });
        }
        if raft_log_id.committed_leader_id().term < last_applied.committed_leader_id().term {
            return Err(ControlPlaneError::ControlPlaneLogTermRegression {
                previous_term: last_applied.committed_leader_id().term,
                actual_term: raft_log_id.committed_leader_id().term,
                index: raft_log_id.index(),
            });
        }
        if raft_log_id.committed_leader_id().term == last_applied.committed_leader_id().term
            && raft_log_id.committed_leader_id().node_id
                < last_applied.committed_leader_id().node_id
        {
            return Err(ControlPlaneError::CommandDecode {
                message: format!(
                    "OpenRaft log id {raft_log_id} is not after last applied log id {last_applied}"
                ),
            });
        }
        Ok(())
    }

    pub fn build_snapshot(&mut self) -> Result<ControlPlaneRaftSnapshot, ControlPlaneError> {
        let artifact = self.inner.build_snapshot_artifact()?;
        let meta = self.snapshot_meta_for_artifact(&artifact)?;
        let snapshot = Snapshot {
            meta,
            snapshot: ControlPlaneRaftSnapshotData::new(artifact.into_payload()),
        };
        publish_control_plane_raft_snapshot(&self.current_snapshot, snapshot.clone())
            .finish_on_current_thread()?;
        Ok(snapshot)
    }

    pub fn create_snapshot_builder(
        &mut self,
    ) -> Result<ControlPlaneRaftSnapshotBuilder, ControlPlaneError> {
        Ok(ControlPlaneRaftSnapshotBuilder::new(self.build_snapshot()?))
    }

    fn capture_snapshot_builder(&self) -> ControlPlaneRaftSnapshotBuilder {
        ControlPlaneRaftSnapshotBuilder::captured(
            ControlPlaneRaftSnapshotBuildInput {
                inner: self.inner.clone(),
                last_applied: self.last_applied,
                last_membership: Arc::clone(&self.last_membership),
                #[cfg(test)]
                hook: self.test_hooks.snapshot_build.clone(),
            },
            Arc::clone(&self.current_snapshot),
        )
    }

    pub fn install_snapshot(
        &mut self,
        meta: &SnapshotMetaOf<ControlPlaneRaftTypeConfig>,
        snapshot: ControlPlaneRaftSnapshotData,
    ) -> Result<(), ControlPlaneError> {
        let last_applied = self.validate_snapshot_meta(meta)?;
        let payload = snapshot.into_inner();
        self.inner
            .install_snapshot_artifact(ControlPlaneSnapshotArtifact::new(
                last_applied,
                payload.clone(),
            ))?;
        self.last_applied = meta.last_log_id;
        self.last_membership = Arc::new(meta.last_membership.clone());
        publish_control_plane_raft_snapshot(
            &self.current_snapshot,
            Snapshot {
                meta: meta.clone(),
                snapshot: ControlPlaneRaftSnapshotData::new(payload),
            },
        )
        .finish_on_current_thread()?;
        Ok(())
    }

    fn snapshot_meta_for_artifact(
        &self,
        artifact: &ControlPlaneSnapshotArtifact,
    ) -> Result<SnapshotMetaOf<ControlPlaneRaftTypeConfig>, ControlPlaneError> {
        Self::snapshot_meta_for_artifact_parts(artifact, self.last_applied, &self.last_membership)
    }

    fn snapshot_meta_for_artifact_parts(
        artifact: &ControlPlaneSnapshotArtifact,
        last_applied: Option<LogIdOf<ControlPlaneRaftTypeConfig>>,
        last_membership: &StoredMembershipOf<ControlPlaneRaftTypeConfig>,
    ) -> Result<SnapshotMetaOf<ControlPlaneRaftTypeConfig>, ControlPlaneError> {
        let last_log_id = match artifact.last_applied() {
            None => match last_applied {
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
                let last_applied = last_applied.ok_or_else(|| {
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
            last_membership: last_membership.clone(),
            snapshot_id: Self::snapshot_id_for_log_id(last_log_id),
        })
    }

    fn snapshot_id_for_log_id(log_id: Option<LogIdOf<ControlPlaneRaftTypeConfig>>) -> String {
        match log_id {
            Some(log_id) => format!(
                "control-plane-T{}-N{}-I{}",
                log_id.committed_leader_id().term,
                log_id.committed_leader_id().node_id,
                log_id.index()
            ),
            None => "control-plane-empty".to_string(),
        }
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
        Self::validate_snapshot_membership_position(meta.last_log_id, &meta.last_membership)?;
        let expected_snapshot_id = Self::snapshot_id_for_log_id(meta.last_log_id);
        if meta.snapshot_id != expected_snapshot_id {
            return Err(ControlPlaneError::SnapshotDecode {
                message: format!(
                    "OpenRaft snapshot id {} does not match expected {} for last_log_id {:?}",
                    meta.snapshot_id, expected_snapshot_id, meta.last_log_id
                ),
            });
        }
        Ok(snapshot_log_id)
    }

    fn validate_restart_log_id_consistency(
        inner: &ReplicatedControlPlaneStateMachine,
        last_applied: Option<LogIdOf<ControlPlaneRaftTypeConfig>>,
    ) -> Result<(), ControlPlaneError> {
        match (inner.last_applied(), last_applied) {
            (None, None) => Ok(()),
            (None, Some(log_id)) if is_openraft_bootstrap_log_id(log_id) => Ok(()),
            (None, Some(log_id)) => Err(ControlPlaneError::SnapshotDecode {
                message: format!(
                    "OpenRaft state machine restart has log id {log_id} but no control-plane last-applied log id"
                ),
            }),
            (Some(control_plane_log_id), Some(log_id))
                if control_plane_log_id_from_raft(log_id) == Some(control_plane_log_id) =>
            {
                Ok(())
            }
            (Some(control_plane_log_id), Some(log_id)) => Err(ControlPlaneError::SnapshotDecode {
                message: format!(
                    "OpenRaft state machine restart log id {log_id} does not match control-plane last-applied {control_plane_log_id:?}"
                ),
            }),
            (Some(control_plane_log_id), None) => Err(ControlPlaneError::SnapshotDecode {
                message: format!(
                    "OpenRaft state machine restart is missing log id for control-plane last-applied {control_plane_log_id:?}"
                ),
            }),
        }
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
    type SnapshotData = ControlPlaneRaftSnapshotData;
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
        let last_applied = self.last_applied;
        let last_membership = Arc::clone(&self.last_membership);
        tokio::task::spawn_blocking(move || (last_applied, last_membership.as_ref().clone()))
            .await
            .map_err(|error| {
                io::Error::other(format!(
                    "control-plane OpenRaft applied-state worker failed: {error}"
                ))
            })
    }

    async fn apply<Strm>(&mut self, mut entries: Strm) -> Result<(), io::Error>
    where
        Strm: Stream<Item = Result<EntryResponder<ControlPlaneRaftTypeConfig>, io::Error>>
            + Unpin
            + OptionalSend,
    {
        while let Some(entry) = entries.next().await {
            let (entry, responder) = entry?;
            let mut candidate = self.capture_apply_candidate();
            #[cfg(test)]
            let hook = self.test_hooks.apply.clone();
            let (candidate, response) = tokio::task::spawn_blocking(move || {
                #[cfg(test)]
                if let Some(hook) = hook {
                    hook.block();
                }
                let response = candidate.apply_entry(entry)?;
                Ok::<_, ControlPlaneError>((candidate, response))
            })
            .await
            .map_err(|error| {
                io::Error::other(format!(
                    "control-plane OpenRaft apply worker failed: {error}"
                ))
            })?
            .map_err(|error| control_plane_error_to_io_error("OpenRaft apply", error))?;
            let retired_inner = std::mem::replace(&mut self.inner, candidate.inner);
            self.last_applied = candidate.last_applied;
            let retired_membership =
                std::mem::replace(&mut self.last_membership, candidate.last_membership);
            ControlPlaneRaftRetiredGeneration {
                inner: retired_inner,
                last_membership: retired_membership,
                cached_snapshot: None,
                #[cfg(test)]
                hook: self.test_hooks.retire_generation.clone(),
            }
            .retire("control-plane OpenRaft applied generation")
            .await?;
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
        self.capture_snapshot_builder()
    }

    async fn begin_receiving_snapshot(
        &mut self,
    ) -> Result<ControlPlaneRaftSnapshotData, io::Error> {
        Ok(ControlPlaneRaftSnapshotData::new(Vec::new()))
    }

    async fn install_snapshot(
        &mut self,
        meta: &SnapshotMetaOf<ControlPlaneRaftTypeConfig>,
        snapshot: ControlPlaneRaftSnapshotData,
    ) -> Result<(), io::Error> {
        let last_applied = self
            .validate_snapshot_meta(meta)
            .map_err(|error| control_plane_error_to_io_error("OpenRaft install snapshot", error))?;
        let cached_snapshot = Snapshot {
            meta: meta.clone(),
            snapshot: snapshot.clone(),
        };
        #[cfg(test)]
        let hook = self.test_hooks.snapshot_install.clone();
        let installed = tokio::task::spawn_blocking(move || {
            #[cfg(test)]
            if let Some(hook) = hook {
                hook.block();
            }
            let payload = snapshot.into_inner();
            let mut installed = ReplicatedControlPlaneStateMachine::empty();
            installed.install_snapshot_artifact(ControlPlaneSnapshotArtifact::new(
                last_applied,
                payload,
            ))?;
            Ok::<_, ControlPlaneError>(installed)
        })
        .await
        .map_err(|error| {
            io::Error::other(format!(
                "control-plane OpenRaft snapshot install worker failed: {error}"
            ))
        })?
        .map_err(|error| control_plane_error_to_io_error("OpenRaft install snapshot", error))?;
        let retired_inner = std::mem::replace(&mut self.inner, installed);
        self.last_applied = meta.last_log_id;
        let retired_membership = std::mem::replace(
            &mut self.last_membership,
            Arc::new(meta.last_membership.clone()),
        );
        let publication =
            publish_control_plane_raft_snapshot(&self.current_snapshot, cached_snapshot);
        let ControlPlaneRaftSnapshotPublication {
            outcome,
            retired: cached_snapshot,
        } = publication;
        ControlPlaneRaftRetiredGeneration {
            inner: retired_inner,
            last_membership: retired_membership,
            cached_snapshot,
            #[cfg(test)]
            hook: self.test_hooks.retire_generation.clone(),
        }
        .retire("control-plane OpenRaft installed generation")
        .await?;
        outcome
            .map_err(|error| control_plane_error_to_io_error("OpenRaft install snapshot", error))?;
        Ok(())
    }

    async fn get_current_snapshot(
        &mut self,
    ) -> Result<Option<ControlPlaneRaftSnapshot>, io::Error> {
        let cache = Arc::clone(&self.current_snapshot);
        tokio::task::spawn_blocking(move || lock_control_plane_raft_snapshot_cache(&cache).clone())
            .await
            .map_err(|error| {
                io::Error::other(format!(
                    "control-plane OpenRaft current-snapshot worker failed: {error}"
                ))
            })
    }
}
