// Copyright The Argmin Authors.
// SPDX-License-Identifier: Apache-2.0

use std::fmt;
use std::future::Future;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::{Duration, Instant};

use tokio::runtime::Handle;

use crate::control_plane::{
    invalidate_authority_clock_restart_checkpoint, load_authority_clock_restart_checkpoint,
    store_authority_clock_restart_checkpoint, ControlPlaneAuthorityClockCheckpointTarget,
    ControlPlaneAuthorityClockRestartCheckpoint, ControlPlaneError,
};
use crate::control_plane_raft::{
    ControlPlaneRaftAuthority, ControlPlaneRaftAuthorityStatus,
    ControlPlaneRaftDurabilityPublication, ControlPlaneRaftLogId, ControlPlaneRaftNodeId,
    ControlPlaneRaftPeerServerCheckpoint, ControlPlaneRaftPeerServerDurability,
};

const DEFAULT_MAX_WAL_SUFFIX_BYTES: u64 = 64 * 1024 * 1024;
const DEFAULT_MAX_MUTATIONS: u64 = 4_096;
const DEFAULT_MAX_DELAY: Duration = Duration::from_secs(60);
const DEFAULT_POLL_INTERVAL: Duration = Duration::from_millis(100);

#[derive(Clone, Copy, Debug)]
struct CheckpointPolicy {
    max_wal_suffix_bytes: u64,
    max_mutations: u64,
    max_delay: Duration,
    poll_interval: Duration,
}

impl Default for CheckpointPolicy {
    fn default() -> Self {
        Self {
            max_wal_suffix_bytes: DEFAULT_MAX_WAL_SUFFIX_BYTES,
            max_mutations: DEFAULT_MAX_MUTATIONS,
            max_delay: DEFAULT_MAX_DELAY,
            poll_interval: DEFAULT_POLL_INTERVAL,
        }
    }
}

impl CheckpointPolicy {
    fn validate(self) -> Result<Self, ControlPlaneError> {
        if self.max_wal_suffix_bytes == 0
            || self.max_mutations == 0
            || self.max_delay.is_zero()
            || self.poll_interval.is_zero()
            || self.poll_interval >= self.max_delay
        {
            return Err(ControlPlaneError::invariant_failure(
                "control-plane Raft checkpoint policy is invalid",
            ));
        }
        Ok(self)
    }
}

#[derive(Clone, Copy, Debug)]
struct CheckpointObservation {
    wal_suffix_bytes: u64,
    successful_append_total: u64,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct CheckpointWork {
    pending_mutations: u64,
    elapsed: Duration,
    wal_suffix_bytes: u64,
}

#[derive(Debug)]
struct CheckpointTracker {
    policy: CheckpointPolicy,
    checkpoint_append_total: u64,
    first_pending_observed_at: Option<Instant>,
}

impl CheckpointTracker {
    fn new(policy: CheckpointPolicy) -> Result<Self, ControlPlaneError> {
        Ok(Self {
            policy: policy.validate()?,
            checkpoint_append_total: 0,
            first_pending_observed_at: None,
        })
    }

    fn observe(
        &mut self,
        now: Instant,
        observation: CheckpointObservation,
    ) -> Option<CheckpointWork> {
        if observation.wal_suffix_bytes == 0 {
            self.checkpoint_append_total = observation.successful_append_total;
            self.first_pending_observed_at = None;
            return None;
        }
        let first_pending_at = self.first_pending_observed_at.get_or_insert(now);
        let work = CheckpointWork {
            pending_mutations: observation
                .successful_append_total
                .saturating_sub(self.checkpoint_append_total),
            elapsed: now.saturating_duration_since(*first_pending_at),
            wal_suffix_bytes: observation.wal_suffix_bytes,
        };
        (work.wal_suffix_bytes >= self.policy.max_wal_suffix_bytes
            || work.pending_mutations >= self.policy.max_mutations
            || work.elapsed
                >= self
                    .policy
                    .max_delay
                    .saturating_sub(self.policy.poll_interval))
        .then_some(work)
    }

    fn complete_checkpoint(&mut self, successful_append_total: u64) {
        self.checkpoint_append_total = successful_append_total;
        self.first_pending_observed_at = None;
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct ServingCheckpointMarker {
    current_leader: Option<ControlPlaneRaftNodeId>,
    persisted_vote: Option<(u64, ControlPlaneRaftNodeId, bool)>,
    current_term: Option<u64>,
    last_log: Option<(u64, ControlPlaneRaftNodeId, u64)>,
    last_purged: Option<(u64, ControlPlaneRaftNodeId, u64)>,
    committed: Option<(u64, ControlPlaneRaftNodeId, u64)>,
    applied: Option<(u64, ControlPlaneRaftNodeId, u64)>,
    current_snapshot: Option<(u64, ControlPlaneRaftNodeId, u64)>,
}

impl ServingCheckpointMarker {
    fn from_status(status: &ControlPlaneRaftAuthorityStatus) -> Self {
        let log_id = |log_id: ControlPlaneRaftLogId| {
            (
                log_id.leader_id.term,
                log_id.leader_id.node_id,
                log_id.index(),
            )
        };
        Self {
            current_leader: status.current_leader(),
            persisted_vote: status
                .persisted_vote()
                .map(|vote| (vote.leader_id.term, vote.leader_id.node_id, vote.committed)),
            current_term: status.current_term(),
            last_log: status.last_log_id().map(log_id),
            last_purged: status.last_purged_log_id().map(log_id),
            committed: status.committed().map(log_id),
            applied: status.applied().map(log_id),
            current_snapshot: status.current_snapshot().map(log_id),
        }
    }
}

pub(crate) struct DurabilityInner {
    runtime: Handle,
    artifact_path: Arc<PathBuf>,
    checkpoint_lock: Mutex<()>,
    serving_checkpoint: Mutex<Option<ServingCheckpointMarker>>,
    monitor_registered: AtomicBool,
}

/// Storage-owned durability lifecycle for one durable Raft authority.
///
/// The artifact path is derived from the authority rather than accepted as a
/// second caller-supplied path. Every checkpoint entry point shares one lock,
/// response-publication domain, serving-read marker, and WAL monitor policy.
#[derive(Clone)]
pub struct ControlPlaneRaftAuthorityDurability {
    authority: Arc<ControlPlaneRaftAuthority>,
    inner: Arc<DurabilityInner>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ControlPlaneRaftOuterIdentityPublicationError {
    InvalidIdentity,
    PersistenceFailure,
}

/// Process-owned publication of the outer static identity marker.
///
/// Storage supplies the exact authority artifact path only after membership,
/// checkpoint, and authority-clock sidecar publication have converged.
pub trait ControlPlaneRaftOuterIdentityPublisher: Send + Sync {
    fn publish(
        &self,
        authority_artifact_path: &Path,
    ) -> Result<(), ControlPlaneRaftOuterIdentityPublicationError>;
}

impl fmt::Debug for ControlPlaneRaftAuthorityDurability {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ControlPlaneRaftAuthorityDurability")
            .field("authority", &"<opaque>")
            .field("artifact", &"<configured>")
            .finish()
    }
}

impl ControlPlaneRaftAuthorityDurability {
    pub(crate) fn issue(
        runtime: Handle,
        authority: Arc<ControlPlaneRaftAuthority>,
    ) -> Result<Self, ControlPlaneError> {
        if let Some(inner) = authority.durability_lifecycle_slot().get() {
            return Ok(Self {
                authority: Arc::clone(&authority),
                inner: Arc::clone(inner),
            });
        }
        let artifact_path = authority
            .configured_durable_artifact_path()
            .map_err(|error| {
                error.into_durability_failure(
                    "control-plane Raft durability lifecycle requires configured durable state",
                )
            })?;
        authority.durability_publication()?;
        let inner = Arc::new(DurabilityInner {
            runtime,
            artifact_path,
            checkpoint_lock: Mutex::new(()),
            serving_checkpoint: Mutex::new(None),
            monitor_registered: AtomicBool::new(false),
        });
        let inner = Arc::clone(authority.durability_lifecycle_slot().get_or_init(|| inner));
        Ok(Self { authority, inner })
    }

    #[cfg(any(test, feature = "test-hooks"))]
    pub(crate) fn shares_lifecycle_for_test(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.inner, &other.inner)
    }

    pub(crate) fn runtime(&self) -> Handle {
        self.inner.runtime.clone()
    }

    pub(crate) fn peer_server_durability(
        &self,
    ) -> Result<ControlPlaneRaftPeerServerDurability, ControlPlaneError> {
        self.authority
            .bind_peer_server_durability(Arc::new(AuthorityPeerCheckpoint {
                durability: self.clone(),
            }))
    }

    #[cfg(any(test, feature = "test-hooks"))]
    pub fn peer_server_durability_for_test(
        &self,
    ) -> Result<ControlPlaneRaftPeerServerDurability, ControlPlaneError> {
        self.peer_server_durability()
    }

    pub(crate) fn store_restart_artifact(&self) -> Result<(), ControlPlaneError> {
        let _guard = self
            .inner
            .checkpoint_lock
            .lock()
            .map_err(|_| checkpoint_lock_error())?;
        self.store_restart_artifact_while_locked()
    }

    #[cfg(any(test, feature = "test-hooks"))]
    pub fn store_restart_artifact_for_test(&self) -> Result<(), ControlPlaneError> {
        self.store_restart_artifact()
    }

    pub(crate) fn checkpoint_successful_linearized_read(&self) -> Result<(), ControlPlaneError> {
        self.publication()?.ensure_available()?;
        if self.authority.durability_metric_snapshots().wal.is_some() {
            let wal_status = self.authority.durable_wal_monitor_snapshot()?;
            if let Some(reason) = wal_status.poisoned() {
                return Err(ControlPlaneError::durability_failure(format!(
                    "durable OpenRaft serving read observed poisoned WAL state: {reason}"
                )));
            }
            return Ok(());
        }

        let status = block_on(&self.inner.runtime, self.authority.status()).ok();
        let marker = status
            .as_ref()
            .filter(|status| status.linearized_authority_serving())
            .map(ServingCheckpointMarker::from_status);
        if marker.is_some()
            && *self
                .inner
                .serving_checkpoint
                .lock()
                .map_err(|_| checkpoint_marker_lock_error())?
                == marker
        {
            return Ok(());
        }
        self.store_restart_artifact()?;
        *self
            .inner
            .serving_checkpoint
            .lock()
            .map_err(|_| checkpoint_marker_lock_error())? = marker;
        Ok(())
    }

    pub(crate) fn authority_clock_checkpoint_target(
        &self,
    ) -> Arc<ControlPlaneAuthorityClockCheckpointTarget> {
        Arc::new(ControlPlaneAuthorityClockCheckpointTarget::new(
            self.inner.artifact_path.as_ref().clone(),
            self.authority.authority_clock_checkpoint_binding(),
        ))
    }

    pub(crate) fn load_authority_clock_restart_checkpoint(
        &self,
    ) -> Result<Option<ControlPlaneAuthorityClockRestartCheckpoint>, ControlPlaneError> {
        load_authority_clock_restart_checkpoint(
            &self.inner.artifact_path,
            self.authority.authority_clock_checkpoint_binding(),
        )
    }

    pub(crate) fn load_authority_clock_restart_checkpoint_for_startup(
        &self,
    ) -> Result<Option<ControlPlaneAuthorityClockRestartCheckpoint>, ControlPlaneError> {
        match self.load_authority_clock_restart_checkpoint() {
            Ok(checkpoint) => Ok(checkpoint),
            Err(error @ ControlPlaneError::AuthorityClockCheckpoint { .. }) => {
                eprintln!(
                    "control-plane authority clock checkpoint is invalid; starting non-serving until authenticated recovery: {error}"
                );
                Ok(None)
            }
            Err(error) => Err(error),
        }
    }

    pub(crate) fn invalidate_authority_clock_restart_checkpoint(
        &self,
    ) -> Result<(), ControlPlaneError> {
        invalidate_authority_clock_restart_checkpoint(&self.inner.artifact_path)
    }

    pub(crate) fn establish_static_outer_identity(
        &self,
        publisher: &dyn ControlPlaneRaftOuterIdentityPublisher,
    ) -> Result<(), ControlPlaneError> {
        loop {
            let guard = self
                .inner
                .checkpoint_lock
                .lock()
                .map_err(|_| checkpoint_lock_error())?;
            let publication = block_on(
                &self.inner.runtime,
                self.authority.publish_static_identity_restart_checkpoint(),
            )?;
            if let Some(publication) = publication {
                store_authority_clock_restart_checkpoint(
                    &self.inner.artifact_path,
                    publication.authority_clock_binding(),
                    1,
                    publication.committed_timestamp_high_water_ms(),
                )?;
                publisher
                    .publish(&self.inner.artifact_path)
                    .map_err(|error| match error {
                        ControlPlaneRaftOuterIdentityPublicationError::InvalidIdentity => {
                            ControlPlaneError::static_topology_failure(
                                "failed to establish static control-plane outer identity",
                            )
                        }
                        ControlPlaneRaftOuterIdentityPublicationError::PersistenceFailure => {
                            ControlPlaneError::durability_failure(
                                "failed to persist established static control-plane outer identity",
                            )
                        }
                    })?;
                return Ok(());
            }
            drop(guard);
            block_on(
                &self.inner.runtime,
                self.authority.wait_for_static_initial_membership(),
            )?;
        }
    }

    #[cfg(any(test, feature = "test-hooks"))]
    pub fn establish_static_outer_identity_for_test(
        &self,
        publisher: &dyn ControlPlaneRaftOuterIdentityPublisher,
    ) -> Result<(), ControlPlaneError> {
        self.establish_static_outer_identity(publisher)
    }

    pub(crate) fn spawn_checkpoint_monitor(
        &self,
        terminal_failure_handler: Arc<dyn Fn() + Send + Sync>,
    ) -> Result<ControlPlaneRaftCheckpointMonitor, ControlPlaneError> {
        self.spawn_checkpoint_monitor_with_policy(
            CheckpointPolicy::default(),
            terminal_failure_handler,
        )
    }

    fn spawn_checkpoint_monitor_with_policy(
        &self,
        policy: CheckpointPolicy,
        terminal_failure_handler: Arc<dyn Fn() + Send + Sync>,
    ) -> Result<ControlPlaneRaftCheckpointMonitor, ControlPlaneError> {
        let mut tracker = CheckpointTracker::new(policy)?;
        self.inner
            .monitor_registered
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
            .map_err(|_| {
                ControlPlaneError::invariant_failure(
                    "control-plane Raft checkpoint monitor is already registered",
                )
            })?;
        let durability = self.clone();
        let handle = thread::spawn(move || loop {
            let publication = match durability.publication() {
                Ok(publication) => publication,
                Err(error) => {
                    eprintln!(
                        "control-plane Raft checkpoint monitor lost its publication domain: {error}"
                    );
                    terminal_failure_handler();
                    return;
                }
            };
            if publication.is_poisoned() {
                return;
            }
            if let Err(error) = durability.checkpoint_wal_if_due(&mut tracker, Instant::now()) {
                publication
                    .poison("control-plane Raft checkpoint monitor observed a durability failure");
                eprintln!(
                    "control-plane Raft bounded WAL checkpoint failed; terminating authority host: {error}"
                );
                terminal_failure_handler();
                return;
            }
            thread::sleep(tracker.policy.poll_interval);
        });
        Ok(ControlPlaneRaftCheckpointMonitor { handle })
    }

    fn checkpoint_wal_if_due(
        &self,
        tracker: &mut CheckpointTracker,
        now: Instant,
    ) -> Result<bool, ControlPlaneError> {
        let wal_status = self.authority.durable_wal_monitor_snapshot()?;
        if let Some(reason) = wal_status.poisoned() {
            return Err(ControlPlaneError::durability_failure(format!(
                "durable OpenRaft checkpoint scheduler observed poisoned WAL state: {reason}"
            )));
        }
        let wal_metrics = wal_status.metrics();
        let successful_append_total = wal_metrics
            .append_total
            .saturating_sub(wal_metrics.append_error_total);
        let offsets = wal_status.offsets();
        let wal_suffix_bytes = offsets
            .clean_len()
            .checked_sub(offsets.base_offset())
            .ok_or_else(|| {
                ControlPlaneError::invariant_failure(format!(
                    "durable OpenRaft WAL clean offset {} precedes base offset {}",
                    offsets.clean_len(),
                    offsets.base_offset()
                ))
            })?;
        if tracker
            .observe(
                now,
                CheckpointObservation {
                    wal_suffix_bytes,
                    successful_append_total,
                },
            )
            .is_none()
        {
            return Ok(false);
        }
        self.snapshot_purge_and_checkpoint()?;
        let completed = self.authority.durable_wal_monitor_snapshot()?.metrics();
        tracker.complete_checkpoint(
            completed
                .append_total
                .saturating_sub(completed.append_error_total),
        );
        Ok(true)
    }

    fn snapshot_purge_and_checkpoint(&self) -> Result<(), ControlPlaneError> {
        let _guard = self
            .inner
            .checkpoint_lock
            .lock()
            .map_err(|_| checkpoint_lock_error())?;
        let snapshot_log_id = block_on(
            &self.inner.runtime,
            self.authority.trigger_local_snapshot_applied(),
        )?;
        self.store_restart_artifact_while_locked()?;
        let Some(snapshot_log_id) = snapshot_log_id else {
            return Ok(());
        };
        let purge_result = block_on(
            &self.inner.runtime,
            self.authority.purge_log_through_snapshot(snapshot_log_id),
        );
        self.store_restart_artifact_while_locked()?;
        purge_result
    }

    fn store_restart_artifact_while_locked(&self) -> Result<(), ControlPlaneError> {
        (|| {
            let artifact_existed = self.inner.artifact_path.exists();
            let committed_timestamp_high_water_ms = block_on(
                &self.inner.runtime,
                self.authority.store_durable_restart_artifact(),
            )?;
            let binding = self.authority.authority_clock_checkpoint_binding();
            if !artifact_existed
                && load_authority_clock_restart_checkpoint(&self.inner.artifact_path, binding)?
                    .is_none()
            {
                store_authority_clock_restart_checkpoint(
                    &self.inner.artifact_path,
                    binding,
                    1,
                    committed_timestamp_high_water_ms,
                )?;
            }
            Ok(())
        })()
        .map_err(|error: ControlPlaneError| {
            error.into_durability_failure("control-plane Raft durable restart checkpoint failed")
        })
    }

    fn publication(&self) -> Result<ControlPlaneRaftDurabilityPublication, ControlPlaneError> {
        self.authority.durability_publication()
    }

    #[cfg(any(test, feature = "test-hooks"))]
    pub fn checkpoint_wal_for_test(
        &self,
        monitor: &mut ControlPlaneRaftCheckpointMonitorForTest,
        now: Instant,
    ) -> Result<bool, ControlPlaneError> {
        self.checkpoint_wal_if_due(&mut monitor.tracker, now)
    }

    #[cfg(any(test, feature = "test-hooks"))]
    pub fn spawn_checkpoint_monitor_for_test(
        &self,
        max_wal_suffix_bytes: u64,
        max_mutations: u64,
        max_delay: Duration,
        poll_interval: Duration,
        terminal_failure_handler: Arc<dyn Fn() + Send + Sync>,
    ) -> Result<ControlPlaneRaftCheckpointMonitor, ControlPlaneError> {
        self.spawn_checkpoint_monitor_with_policy(
            CheckpointPolicy {
                max_wal_suffix_bytes,
                max_mutations,
                max_delay,
                poll_interval,
            },
            terminal_failure_handler,
        )
    }

    #[cfg(any(test, feature = "test-hooks"))]
    #[must_use]
    pub fn default_checkpoint_max_delay_for_test() -> Duration {
        DEFAULT_MAX_DELAY
    }

    #[cfg(any(test, feature = "test-hooks"))]
    pub fn block_checkpoints_for_test(&self) -> ControlPlaneRaftCheckpointBlockForTest<'_> {
        ControlPlaneRaftCheckpointBlockForTest {
            _guard: self.inner.checkpoint_lock.lock().unwrap(),
        }
    }
}

struct AuthorityPeerCheckpoint {
    durability: ControlPlaneRaftAuthorityDurability,
}

impl ControlPlaneRaftPeerServerCheckpoint for AuthorityPeerCheckpoint {
    fn checkpoint_before_snapshot_response(
        &self,
        _authority: &ControlPlaneRaftAuthority,
    ) -> Result<(), ControlPlaneError> {
        self.durability.store_restart_artifact()
    }
}

#[must_use = "dropping the handle detaches the checkpoint monitor"]
pub struct ControlPlaneRaftCheckpointMonitor {
    handle: thread::JoinHandle<()>,
}

impl fmt::Debug for ControlPlaneRaftCheckpointMonitor {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ControlPlaneRaftCheckpointMonitor")
            .field("running", &!self.handle.is_finished())
            .finish()
    }
}

#[cfg(any(test, feature = "test-hooks"))]
impl ControlPlaneRaftCheckpointMonitor {
    pub fn join_for_test(self) -> thread::Result<()> {
        self.handle.join()
    }
}

#[cfg(any(test, feature = "test-hooks"))]
pub struct ControlPlaneRaftCheckpointMonitorForTest {
    tracker: CheckpointTracker,
}

#[cfg(any(test, feature = "test-hooks"))]
pub struct ControlPlaneRaftCheckpointBlockForTest<'a> {
    _guard: std::sync::MutexGuard<'a, ()>,
}

#[cfg(any(test, feature = "test-hooks"))]
impl ControlPlaneRaftCheckpointMonitorForTest {
    pub fn new(
        max_wal_suffix_bytes: u64,
        max_mutations: u64,
        max_delay: Duration,
        poll_interval: Duration,
    ) -> Result<Self, ControlPlaneError> {
        Ok(Self {
            tracker: CheckpointTracker::new(CheckpointPolicy {
                max_wal_suffix_bytes,
                max_mutations,
                max_delay,
                poll_interval,
            })?,
        })
    }
}

#[cfg(any(test, feature = "test-hooks"))]
impl Default for ControlPlaneRaftCheckpointMonitorForTest {
    fn default() -> Self {
        Self {
            tracker: CheckpointTracker::new(CheckpointPolicy::default())
                .expect("default checkpoint policy should remain valid"),
        }
    }
}

fn checkpoint_lock_error() -> ControlPlaneError {
    ControlPlaneError::durability_failure("control-plane Raft checkpoint lock is unavailable")
}

fn checkpoint_marker_lock_error() -> ControlPlaneError {
    ControlPlaneError::durability_failure(
        "control-plane Raft serving-checkpoint marker is unavailable",
    )
}

fn block_on<F: Future>(runtime: &Handle, future: F) -> F::Output {
    if Handle::try_current().is_ok() {
        tokio::task::block_in_place(|| runtime.block_on(future))
    } else {
        runtime.block_on(future)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn runtime() -> tokio::runtime::Runtime {
        tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .build()
            .expect("test runtime should build")
    }

    #[test]
    fn checkpoint_tracker_enforces_each_bound() {
        let mut tracker = CheckpointTracker::new(CheckpointPolicy {
            max_wal_suffix_bytes: 100,
            max_mutations: 2,
            max_delay: Duration::from_secs(1),
            poll_interval: Duration::from_millis(100),
        })
        .unwrap();
        let started = Instant::now();
        assert!(tracker
            .observe(
                started,
                CheckpointObservation {
                    wal_suffix_bytes: 0,
                    successful_append_total: 10,
                },
            )
            .is_none());
        assert!(tracker
            .observe(
                started,
                CheckpointObservation {
                    wal_suffix_bytes: 99,
                    successful_append_total: 11,
                },
            )
            .is_none());
        assert_eq!(
            tracker.observe(
                started,
                CheckpointObservation {
                    wal_suffix_bytes: 100,
                    successful_append_total: 11,
                },
            ),
            Some(CheckpointWork {
                pending_mutations: 1,
                elapsed: Duration::ZERO,
                wal_suffix_bytes: 100,
            })
        );

        tracker.complete_checkpoint(11);
        assert!(tracker
            .observe(
                started,
                CheckpointObservation {
                    wal_suffix_bytes: 1,
                    successful_append_total: 13,
                },
            )
            .is_some_and(|work| work.pending_mutations == 2));

        tracker.complete_checkpoint(13);
        assert!(tracker
            .observe(
                started,
                CheckpointObservation {
                    wal_suffix_bytes: 1,
                    successful_append_total: 14,
                },
            )
            .is_none());
        assert!(tracker
            .observe(
                started + Duration::from_millis(900),
                CheckpointObservation {
                    wal_suffix_bytes: 1,
                    successful_append_total: 14,
                },
            )
            .is_some_and(|work| work.elapsed == Duration::from_millis(900)));
    }

    #[test]
    fn durability_derives_the_artifact_path_and_initializes_its_clock_sidecar() {
        let tmp = test_util::tempdir();
        let artifact_path = tmp.path().join("authority.state");
        let secret_cluster_name = "secret-durability-cluster";
        let runtime = runtime();
        let authority = Arc::new(
            runtime
                .block_on(
                    ControlPlaneRaftAuthority::new_experimental_single_node_durable(
                        secret_cluster_name,
                        1,
                        &artifact_path,
                    ),
                )
                .expect("durable authority should initialize"),
        );
        let durability = authority
            .durability_lifecycle(runtime.handle().clone())
            .expect("durability lifecycle should bind to the authority");
        let repeated = authority
            .durability_lifecycle(runtime.handle().clone())
            .expect("repeated issuance should return the authority's existing lifecycle");
        assert!(
            Arc::ptr_eq(&durability.inner, &repeated.inner),
            "one authority must retain one checkpoint lock and serving-marker domain"
        );

        let debug = format!("{durability:?}");
        assert!(!debug.contains(secret_cluster_name), "{debug}");
        assert!(
            !debug.contains(artifact_path.to_string_lossy().as_ref()),
            "{debug}"
        );
        durability
            .store_restart_artifact()
            .expect("storage-owned checkpoint should persist");
        assert!(artifact_path.is_file());
        assert!(durability
            .load_authority_clock_restart_checkpoint()
            .expect("authority-clock sidecar should load")
            .is_some());

        runtime
            .block_on(authority.shutdown())
            .expect("authority should shut down");
    }

    #[test]
    fn durability_allows_only_one_checkpoint_monitor() {
        let tmp = test_util::tempdir();
        let artifact_path = tmp.path().join("authority.state");
        let runtime = runtime();
        let authority = Arc::new(
            runtime
                .block_on(
                    ControlPlaneRaftAuthority::new_experimental_single_node_durable(
                        "single-checkpoint-monitor",
                        1,
                        &artifact_path,
                    ),
                )
                .expect("durable authority should initialize"),
        );
        let durability = authority
            .durability_lifecycle(runtime.handle().clone())
            .expect("durability lifecycle should bind to the authority");
        let monitor = durability
            .spawn_checkpoint_monitor_for_test(
                u64::MAX,
                u64::MAX,
                Duration::from_millis(100),
                Duration::from_millis(10),
                Arc::new(|| {}),
            )
            .expect("the authority's first checkpoint monitor should start");
        let error = durability
            .spawn_checkpoint_monitor_for_test(
                u64::MAX,
                u64::MAX,
                Duration::from_millis(100),
                Duration::from_millis(10),
                Arc::new(|| {}),
            )
            .expect_err("a second monitor must not acquire an independent tracker");
        assert!(matches!(error, ControlPlaneError::InvariantFailure { .. }));

        authority
            .durability_publication()
            .expect("durability publication should exist")
            .poison("stop single-monitor test");
        monitor
            .join_for_test()
            .expect("checkpoint monitor should stop after authority poison");
        drop(durability);
        let reissued = authority
            .durability_lifecycle(runtime.handle().clone())
            .expect("authority should retain its durability lifecycle");
        let error = reissued
            .spawn_checkpoint_monitor_for_test(
                u64::MAX,
                u64::MAX,
                Duration::from_millis(100),
                Duration::from_millis(10),
                Arc::new(|| {}),
            )
            .expect_err("dropping a handle must not reset monitor registration");
        assert!(matches!(error, ControlPlaneError::InvariantFailure { .. }));
        runtime
            .block_on(authority.shutdown())
            .expect("authority should shut down");
    }

    #[test]
    fn durability_rejects_an_authority_without_a_configured_artifact() {
        let runtime = runtime();
        let authority = Arc::new(
            runtime
                .block_on(
                    ControlPlaneRaftAuthority::new_experimental_single_node_in_memory(
                        "in-memory-durability",
                        1,
                    ),
                )
                .expect("in-memory authority should initialize"),
        );
        let error = authority
            .durability_lifecycle(runtime.handle().clone())
            .expect_err("durability lifecycle requires an authority-owned artifact path");
        assert!(matches!(error, ControlPlaneError::DurabilityFailure { .. }));
        runtime
            .block_on(authority.shutdown())
            .expect("authority should shut down");
    }
}
