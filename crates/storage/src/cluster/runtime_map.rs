// Copyright The Argmin Authors.
// SPDX-License-Identifier: Apache-2.0

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct StorageClusterRuntimeMapRefreshLoopSuccess {
    pub cluster_epoch: ClusterEpoch,
    pub route_map_validity: RouteMapValidity,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct StorageClusterRuntimeMapRefreshLoopFailure {
    pub attempt: u64,
    pub kind: &'static str,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct StorageClusterRuntimeMapRefreshLoopStatus {
    pub attempts: u64,
    pub successes: u64,
    pub failures: u64,
    pub fallback_recovery_attempts: u64,
    pub fallback_recovery_failures: u64,
    pub last_success: Option<StorageClusterRuntimeMapRefreshLoopSuccess>,
    pub last_failure: Option<StorageClusterRuntimeMapRefreshLoopFailure>,
    pub last_error: Option<String>,
}

pub struct StorageClusterRuntimeMapRefreshLoop {
    stop: Arc<(Mutex<bool>, Condvar)>,
    status: Arc<Mutex<StorageClusterRuntimeMapRefreshLoopStatus>>,
    handles: Vec<JoinHandle<()>>,
}

#[derive(Clone)]
pub struct StorageClusterRuntimeMapRefreshLoopStatusHandle {
    status: Arc<Mutex<StorageClusterRuntimeMapRefreshLoopStatus>>,
}

impl StorageClusterRuntimeMapRefreshLoopStatusHandle {
    #[must_use]
    pub fn status(&self) -> StorageClusterRuntimeMapRefreshLoopStatus {
        self.status
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .clone()
    }
}

impl StorageClusterRuntimeMapRefreshLoop {
    #[must_use]
    pub fn status_handle(&self) -> StorageClusterRuntimeMapRefreshLoopStatusHandle {
        StorageClusterRuntimeMapRefreshLoopStatusHandle {
            status: Arc::clone(&self.status),
        }
    }

    pub fn status(&self) -> StorageClusterRuntimeMapRefreshLoopStatus {
        self.status_handle().status()
    }

    pub fn stop(&mut self) {
        stop_runtime_map_workers(&self.stop);
        for handle in self.handles.drain(..) {
            let _ = handle.join();
        }
    }
}

impl Drop for StorageClusterRuntimeMapRefreshLoop {
    fn drop(&mut self) {
        self.stop();
    }
}

const PENDING_METADATA_COMMAND_FALLBACK_RETRY_INTERVAL: Duration = Duration::from_secs(5);

#[derive(Default)]
struct PendingMetadataCommandFallbackSchedule {
    attempted_request_generation: u64,
    retry_not_before: Option<Instant>,
}

impl PendingMetadataCommandFallbackSchedule {
    fn should_attempt(&self, request_generation: u64, now: Instant) -> bool {
        request_generation > self.attempted_request_generation
            || self
                .retry_not_before
                .is_some_and(|deadline| now >= deadline)
    }

    fn record_attempt(&mut self, request_generation: u64, now: Instant, succeeded: bool) {
        self.attempted_request_generation = request_generation;
        self.retry_not_before = (!succeeded)
            .then_some(now + PENDING_METADATA_COMMAND_FALLBACK_RETRY_INTERVAL);
    }
}

#[cfg(test)]
mod fallback_schedule_tests {
    use super::*;

    #[test]
    fn failed_fallback_retries_only_after_cooldown_and_new_outages_run_immediately() {
        let started = Instant::now();
        let mut schedule = PendingMetadataCommandFallbackSchedule::default();

        assert!(schedule.should_attempt(1, started));
        schedule.record_attempt(1, started, false);
        assert!(!schedule.should_attempt(
            1,
            started + PENDING_METADATA_COMMAND_FALLBACK_RETRY_INTERVAL - Duration::from_millis(1)
        ));
        assert!(schedule.should_attempt(
            1,
            started + PENDING_METADATA_COMMAND_FALLBACK_RETRY_INTERVAL
        ));

        let retry = started + PENDING_METADATA_COMMAND_FALLBACK_RETRY_INTERVAL;
        schedule.record_attempt(1, retry, true);
        assert!(!schedule.should_attempt(1, retry + Duration::from_secs(30)));
        assert!(
            schedule.should_attempt(2, retry),
            "a new outage must not inherit the previous outage's cooldown"
        );
    }
}

fn stop_runtime_map_workers(stop: &Arc<(Mutex<bool>, Condvar)>) {
    let (lock, cvar) = &**stop;
    let mut stopped = lock.lock().unwrap_or_else(|poisoned| poisoned.into_inner());
    *stopped = true;
    cvar.notify_all();
}

fn wait_for_runtime_map_worker(
    stop: &Arc<(Mutex<bool>, Condvar)>,
    interval: Duration,
) -> bool {
    let (lock, cvar) = &**stop;
    let stopped = lock.lock().unwrap_or_else(|poisoned| poisoned.into_inner());
    if *stopped {
        return true;
    }
    let (stopped, _) = cvar
        .wait_timeout(stopped, interval)
        .unwrap_or_else(|poisoned| poisoned.into_inner());
    *stopped
}

impl StorageClusterRouteHandle {
    pub(crate) fn from_authorized_cluster(initial: Arc<StorageCluster>) -> Self {
        Self {
            same_epoch_generations: Arc::new(Mutex::new(vec![Arc::downgrade(&initial)])),
            cluster: Arc::new(RwLock::new(initial)),
            route_admission: StorageClusterRouteAdmissionGate::default(),
        }
    }

    /// Construct request-admission capability for a static storage cluster.
    ///
    /// Dynamically published clusters must instead retain a
    /// [`StorageClusterRuntimeMapHandle`] and derive this capability through
    /// [`StorageClusterRuntimeMapHandle::route_handle`].
    pub fn from_static_cluster(initial: Arc<StorageCluster>) -> Result<Self, ClusterBuildError> {
        initial.route_authority.require_static()?;
        Ok(Self::from_authorized_cluster(initial))
    }

    pub fn current(&self) -> Arc<StorageCluster> {
        self.cluster
            .read()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .clone()
    }

    /// Revalidate an admission against this exact frontend publication domain
    /// and its currently installed runtime-map generation.
    pub fn require_admission_valid_now(
        &self,
        admission: &StorageClusterRouteAdmission,
    ) -> Result<(), crate::StoreFailure> {
        self.require_admission_valid_now_raw(admission)
            .map_err(Into::into)
    }

    fn require_admission_valid_now_raw(
        &self,
        admission: &StorageClusterRouteAdmission,
    ) -> Result<(), StoreError> {
        let cluster = self.current();
        if !Arc::ptr_eq(&self.route_admission.inner, &admission._permit.gate.inner) {
            return Err(StoreError::RouteAdmissionClusterMismatch {
                admitted_epoch: admission.cluster.cluster_epoch(),
                operation_epoch: cluster.cluster_epoch(),
            });
        }
        admission.require_valid_now_for_raw(&cluster)
    }

    /// Return whether both handles participate in the same frontend
    /// route-publication admission domain.
    ///
    /// A frontend pool must share this domain so one request admission
    /// prevents every worker from switching to a replacement runtime map.
    pub fn shares_route_admission_with(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.route_admission.inner, &other.route_admission.inner)
    }

    pub(crate) fn route_admission_domain(&self) -> StorageClusterRouteAdmissionDomain {
        StorageClusterRouteAdmissionDomain {
            gate: Arc::downgrade(&self.route_admission.inner),
        }
    }

    #[cfg(any(test, feature = "test-hooks"))]
    pub(crate) fn test_wait_until_route_request_is_admitted(&self) {
        self.route_admission.wait_until_request_is_admitted();
    }

    #[cfg(any(test, feature = "test-hooks"))]
    pub(crate) fn test_wait_until_route_publication_is_pending(&self) {
        self.route_admission.wait_until_publication_is_pending();
    }

    pub fn admit_current_route(&self) -> Result<StorageClusterRouteAdmission, crate::StoreFailure> {
        self.admit_current_route_raw().map_err(Into::into)
    }

    fn admit_current_route_raw(&self) -> Result<StorageClusterRouteAdmission, StoreError> {
        let permit = self.route_admission.acquire();
        let cluster = self.current();
        let admitted_lease = cluster.local_map.route_map_lease_snapshot();
        let admission = StorageClusterRouteAdmission {
            admitted_lease,
            cluster,
            _permit: permit,
        };
        admission.require_valid_now_raw()?;
        Ok(admission)
    }

    #[cfg(test)]
    fn admit_current_route_with_lease_capture_hook<F>(
        &self,
        after_lease_read_lock: F,
    ) -> Result<StorageClusterRouteAdmission, StoreError>
    where
        F: FnOnce(),
    {
        let permit = self.route_admission.acquire();
        let cluster = self.current();
        let admitted_lease = cluster
            .local_map
            .route_map_lease_snapshot_with_hook(after_lease_read_lock);
        let admission = StorageClusterRouteAdmission {
            admitted_lease,
            cluster,
            _permit: permit,
        };
        admission.require_valid_now_raw()?;
        Ok(admission)
    }

    pub(crate) fn install(
        &self,
        candidate: Arc<StorageCluster>,
    ) -> Result<(), StorageClusterRuntimeMapRefreshError> {
        self.install_with_after_drain(candidate, || {})
    }

    fn install_with_after_drain(
        &self,
        candidate: Arc<StorageCluster>,
        after_drain: impl FnOnce(),
    ) -> Result<(), StorageClusterRuntimeMapRefreshError> {
        self.install_if_current_with_after_drain(None, candidate, after_drain)
            .map(|installed| {
                debug_assert!(installed, "unconditional route publication must install");
            })
    }

    #[cfg(any(test, feature = "test-hooks"))]
    fn test_install_if_current(
        &self,
        expected_current: &Arc<StorageCluster>,
        candidate: Arc<StorageCluster>,
    ) -> Result<bool, StorageClusterRuntimeMapRefreshError> {
        self.install_if_current_with_after_drain(Some(expected_current), candidate, || {})
    }

    fn install_if_current_with_after_drain(
        &self,
        expected_current: Option<&Arc<StorageCluster>>,
        candidate: Arc<StorageCluster>,
        after_drain: impl FnOnce(),
    ) -> Result<bool, StorageClusterRuntimeMapRefreshError> {
        candidate.route_authority.dynamic_proof()?;
        let publication = self.route_admission.begin_publication();
        after_drain();
        self.install_if_current_during_publication(
            &publication,
            expected_current,
            candidate,
        )
    }

    fn install_if_current_during_publication(
        &self,
        publication: &StorageClusterRoutePublicationGuard,
        expected_current: Option<&Arc<StorageCluster>>,
        candidate: Arc<StorageCluster>,
    ) -> Result<bool, StorageClusterRuntimeMapRefreshError> {
        debug_assert!(Arc::ptr_eq(
            &self.route_admission.inner,
            &publication.gate.inner
        ));
        let candidate_authority = candidate.route_authority.dynamic_proof()?;
        let mut current = self
            .cluster
            .write()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        current.route_authority.dynamic_proof()?;
        if expected_current.is_some_and(|expected| !Arc::ptr_eq(expected, &current)) {
            return Ok(false);
        }
        if candidate.cluster_epoch() < current.cluster_epoch() {
            return Err(StorageClusterRuntimeMapRefreshError::EpochDowngrade {
                current: current.cluster_epoch(),
                candidate: candidate.cluster_epoch(),
            });
        }
        let mut generations = self
            .same_epoch_generations
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        if let Err(error) = candidate.require_route_map_valid_now() {
            let (valid_until_ms, now_ms) = match error {
                StoreError::RouteMapExpired {
                    valid_until_ms,
                    now_ms,
                    ..
                } => (valid_until_ms, now_ms),
                _ => (
                    candidate.route_map_valid_until_ms().unwrap_or(0),
                    crate::clock::current_time_millis(),
                ),
            };
            return Err(
                StorageClusterRuntimeMapRefreshError::ExpiredRouteMapValidity {
                    candidate: candidate.cluster_epoch(),
                    valid_until_ms,
                    now_ms,
                },
            );
        }
        if candidate.cluster_epoch() == current.cluster_epoch() {
            let candidate_digest = candidate_authority.content_digest;
            let candidate_authority_incarnation =
                candidate_authority.freshness_proof.authority_incarnation();
            // Same-epoch authoritative refreshes update matching pinned
            // dynamic generations only.
            generations.retain(|generation| {
                let Some(generation) = generation.upgrade() else {
                    return false;
                };
                if generation.cluster_epoch() == candidate.cluster_epoch() {
                    if !matches!(
                        generation.route_authority,
                        StorageClusterRouteAuthority::Dynamic(proof)
                            if proof.content_digest == candidate_digest
                                && proof.freshness_proof.authority_incarnation()
                                    == candidate_authority_incarnation
                    ) {
                        return true;
                    }
                    generation
                        .local_map
                        .replace_route_map_lease_from(&candidate.local_map);
                    true
                } else {
                    false
                }
            });
            generations.push(Arc::downgrade(&candidate));
        } else {
            generations.clear();
            generations.push(Arc::downgrade(&candidate));
        }
        *current = candidate;
        Ok(true)
    }

    fn renew_from_runtime_map_status(
        &self,
        status: ControlPlaneRuntimeMapStatus,
        local_wall_ms: u64,
        local_monotonic_ms: u64,
    ) -> Result<Option<Arc<StorageCluster>>, StorageClusterRuntimeMapRefreshError> {
        let current = self
            .cluster
            .write()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let current_authority = current.route_authority.dynamic_proof()?;
        let Some(renewal) = status.lease_renewal() else {
            return Ok(None);
        };
        if status.cluster_epoch() != current.cluster_epoch()
            || current_authority.content_digest != renewal.content_digest()
            || current_authority.freshness_proof.authority_incarnation()
                != renewal.freshness_proof().authority_incarnation()
        {
            return Ok(None);
        }
        let Some(_) = renewal.validity().valid_until_ms() else {
            return Err(
                StorageClusterRuntimeMapRefreshError::UnboundedRouteMapValidity {
                    candidate: status.cluster_epoch(),
                },
            );
        };
        let bound_lease = renewal
            .bind_process_local_lease_at(local_wall_ms, local_monotonic_ms)
            .map_err(|error| ClusterBuildError::RouteMapLeaseBinding {
                message: error.to_string(),
            })?;
        let validity = renewal.validity();
        let digest = renewal.content_digest();
        let authority_incarnation = renewal.freshness_proof().authority_incarnation();
        let mut generations = self
            .same_epoch_generations
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        generations.retain(|generation| {
            let Some(generation) = generation.upgrade() else {
                return false;
            };
            if generation.cluster_epoch() != status.cluster_epoch()
                || !matches!(
                    generation.route_authority,
                    StorageClusterRouteAuthority::Dynamic(proof)
                        if proof.content_digest == digest
                            && proof.freshness_proof.authority_incarnation()
                                == authority_incarnation
                )
            {
                return true;
            }
            generation.replace_route_map_lease(validity, bound_lease);
            true
        });
        Ok(Some(Arc::clone(&current)))
    }

    fn expire_same_epoch_generations(&self, now_ms: u64) {
        let current_epoch = self.current().cluster_epoch();
        let expiry = RouteMapValidity::until_ms_saturating(now_ms);
        let local_monotonic_ms = crate::clock::monotonic_time_millis();
        let mut generations = self
            .same_epoch_generations
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        generations.retain(|generation| {
            let Some(generation) = generation.upgrade() else {
                return false;
            };
            if generation.cluster_epoch() == current_epoch {
                generation
                    .local_map
                    .expire_route_map_lease_at(expiry, local_monotonic_ms);
                true
            } else {
                false
            }
        });
    }

    pub(crate) fn refresh_from_control_plane_runtime_map(
        &self,
        control_plane: &impl ControlPlaneRuntimeMapSource,
        authority_now_ms: u64,
    ) -> Result<Arc<StorageCluster>, StorageClusterRuntimeMapRefreshError> {
        let started = Instant::now();
        let mut first_sample = true;
        self.refresh_from_control_plane_runtime_map_with_authority_clock(control_plane, || {
            if std::mem::take(&mut first_sample) {
                return authority_now_ms;
            }
            let elapsed_ms = u64::try_from(started.elapsed().as_millis()).unwrap_or(u64::MAX);
            authority_now_ms.saturating_add(elapsed_ms)
        })
    }

    pub(crate) fn refresh_from_control_plane_runtime_map_with_authority_clock<F>(
        &self,
        control_plane: &impl ControlPlaneRuntimeMapSource,
        mut authority_now_ms: F,
    ) -> Result<Arc<StorageCluster>, StorageClusterRuntimeMapRefreshError>
    where
        F: FnMut() -> u64,
    {
        let status = control_plane.runtime_map_status(authority_now_ms())?;
        let local_wall_ms = crate::clock::current_time_millis();
        let local_monotonic_ms = crate::clock::monotonic_time_millis();
        if let Some(current) =
            self.renew_from_runtime_map_status(status, local_wall_ms, local_monotonic_ms)?
        {
            return Ok(current);
        }
        let publication = self.route_admission.begin_publication();
        let candidate = self
            .current()
            .refresh_from_control_plane_runtime_map(control_plane, authority_now_ms())?;
        self.install_if_current_during_publication(&publication, None, Arc::clone(&candidate))?;
        Ok(candidate)
    }

    pub(crate) fn refresh_from_control_plane_runtime_map_with_unix_storage_node_clients(
        &self,
        control_plane: &impl ControlPlaneRuntimeMapSource,
        authority_now_ms: u64,
        admission_settings: LocalUnixStorageNodeClientAdmissionSettings,
    ) -> Result<Arc<StorageCluster>, StorageClusterRuntimeMapRefreshError> {
        let started = Instant::now();
        let mut first_sample = true;
        self.refresh_from_control_plane_runtime_map_with_unix_storage_node_clients_and_authority_clock(
            control_plane,
            admission_settings,
            || {
                if std::mem::take(&mut first_sample) {
                    return authority_now_ms;
                }
                let elapsed_ms =
                    u64::try_from(started.elapsed().as_millis()).unwrap_or(u64::MAX);
                authority_now_ms.saturating_add(elapsed_ms)
            },
        )
    }

    pub(crate) fn refresh_from_control_plane_runtime_map_with_unix_storage_node_clients_and_authority_clock<
        F,
    >(
        &self,
        control_plane: &impl ControlPlaneRuntimeMapSource,
        admission_settings: LocalUnixStorageNodeClientAdmissionSettings,
        mut authority_now_ms: F,
    ) -> Result<Arc<StorageCluster>, StorageClusterRuntimeMapRefreshError>
    where
        F: FnMut() -> u64,
    {
        let status = control_plane.runtime_map_status(authority_now_ms())?;
        let local_wall_ms = crate::clock::current_time_millis();
        let local_monotonic_ms = crate::clock::monotonic_time_millis();
        if let Some(current) =
            self.renew_from_runtime_map_status(status, local_wall_ms, local_monotonic_ms)?
        {
            return Ok(current);
        }
        let publication = self.route_admission.begin_publication();
        let candidate = self
            .current()
            .refresh_from_control_plane_runtime_map_with_unix_storage_node_clients(
                control_plane,
                authority_now_ms(),
                admission_settings,
            )?;
        self.install_if_current_during_publication(&publication, None, Arc::clone(&candidate))?;
        Ok(candidate)
    }

    pub(crate) fn spawn_control_plane_refresh_loop<S, F>(
        self,
        control_plane: S,
        refresh_interval: Duration,
        authority_now_ms: F,
    ) -> Result<StorageClusterRuntimeMapRefreshLoop, StorageClusterRuntimeMapRefreshError>
    where
        S: ControlPlaneRuntimeMapSource + Send + Sync + 'static,
        F: Fn() -> u64 + Send + Sync + 'static,
    {
        self.spawn_control_plane_refresh_loop_inner(
            control_plane,
            refresh_interval,
            authority_now_ms,
            None,
            true,
        )
    }

    pub(crate) fn spawn_control_plane_refresh_loop_with_unix_storage_node_clients<S, F>(
        self,
        control_plane: S,
        refresh_interval: Duration,
        authority_now_ms: F,
        admission_settings: LocalUnixStorageNodeClientAdmissionSettings,
    ) -> Result<StorageClusterRuntimeMapRefreshLoop, StorageClusterRuntimeMapRefreshError>
    where
        S: ControlPlaneRuntimeMapSource + Send + Sync + 'static,
        F: Fn() -> u64 + Send + Sync + 'static,
    {
        self.spawn_control_plane_refresh_loop_inner(
            control_plane,
            refresh_interval,
            authority_now_ms,
            Some(admission_settings),
            true,
        )
    }

    pub(crate) fn spawn_control_plane_refresh_only_loop_with_unix_storage_node_clients<S, F>(
        self,
        control_plane: S,
        refresh_interval: Duration,
        authority_now_ms: F,
        admission_settings: LocalUnixStorageNodeClientAdmissionSettings,
    ) -> Result<StorageClusterRuntimeMapRefreshLoop, StorageClusterRuntimeMapRefreshError>
    where
        S: ControlPlaneRuntimeMapSource + Send + Sync + 'static,
        F: Fn() -> u64 + Send + Sync + 'static,
    {
        self.spawn_control_plane_refresh_loop_inner(
            control_plane,
            refresh_interval,
            authority_now_ms,
            Some(admission_settings),
            false,
        )
    }

    fn spawn_control_plane_refresh_loop_inner<S, F>(
        self,
        control_plane: S,
        refresh_interval: Duration,
        authority_now_ms: F,
        admission_settings: Option<LocalUnixStorageNodeClientAdmissionSettings>,
        recover_pending_commands: bool,
    ) -> Result<StorageClusterRuntimeMapRefreshLoop, StorageClusterRuntimeMapRefreshError>
    where
        S: ControlPlaneRuntimeMapSource + Send + Sync + 'static,
        F: Fn() -> u64 + Send + Sync + 'static,
    {
        if refresh_interval.is_zero() {
            return Err(StorageClusterRuntimeMapRefreshError::RefreshLoopZeroInterval);
        }

        let stop = Arc::new((Mutex::new(false), Condvar::new()));
        let status = Arc::new(Mutex::new(
            StorageClusterRuntimeMapRefreshLoopStatus::default(),
        ));
        let control_plane = Arc::new(control_plane);
        let authority_now_ms = Arc::new(authority_now_ms);
        let recovery_request_generation = Arc::new(AtomicU64::new(0));
        let refresh_outage_active = Arc::new(AtomicBool::new(false));
        let worker_stop = Arc::clone(&stop);
        let worker_status = Arc::clone(&status);
        let worker_control_plane = Arc::clone(&control_plane);
        let worker_authority_now_ms = Arc::clone(&authority_now_ms);
        let worker_recovery_request_generation = Arc::clone(&recovery_request_generation);
        let worker_refresh_outage_active = Arc::clone(&refresh_outage_active);
        let refresh_route_handle = self.clone();
        // Lease renewal must never share a worker with metadata recovery. A
        // recovery RPC may consume its full retry budget, which can be longer
        // than the usable route-map lease after the clock-skew reserve.
        let refresh_handle = thread::Builder::new()
            .name("argmin-storage-cluster-control-plane-refresh".to_string())
            .spawn(move || loop {
                let refresh_now_ms = worker_authority_now_ms();
                let result = match admission_settings {
                    Some(admission_settings) => refresh_route_handle
                        .refresh_from_control_plane_runtime_map_with_unix_storage_node_clients(
                            worker_control_plane.as_ref(),
                            refresh_now_ms,
                            admission_settings,
                        ),
                    None => refresh_route_handle.refresh_from_control_plane_runtime_map(
                        worker_control_plane.as_ref(),
                        refresh_now_ms,
                    ),
                };
                if let Err(error) = &result {
                    if recover_pending_commands
                        && !worker_refresh_outage_active.swap(true, Ordering::AcqRel)
                    {
                        let _ = worker_recovery_request_generation.fetch_update(
                            Ordering::AcqRel,
                            Ordering::Acquire,
                            |generation| generation.checked_add(1),
                        );
                    }
                    if runtime_map_refresh_error_requires_current_map_invalidation(error) {
                        refresh_route_handle.expire_same_epoch_generations(refresh_now_ms);
                    }
                    let _ = observability::emit_flight_event(
                        "storage",
                        "runtime_map_refresh_error",
                        format!(
                            "kind={} error={error}",
                            error.diagnostic_kind()
                        ),
                    );
                    let _ = observability::event(
                        "storage",
                        "runtime_map_refresh_error",
                        Some(format_args!(
                            "kind={} error={error}",
                            error.diagnostic_kind()
                        )),
                    );
                } else {
                    worker_refresh_outage_active.store(false, Ordering::Release);
                }
                {
                    let mut status = worker_status
                        .lock()
                        .unwrap_or_else(|poisoned| poisoned.into_inner());
                    status.attempts += 1;
                    if let Err(error) = &result {
                        status.last_failure =
                            Some(StorageClusterRuntimeMapRefreshLoopFailure {
                                attempt: status.attempts,
                                kind: error.diagnostic_kind(),
                            });
                    }
                    match result {
                        Ok(cluster) => {
                            status.successes += 1;
                            status.last_success =
                                Some(StorageClusterRuntimeMapRefreshLoopSuccess {
                                    cluster_epoch: cluster.cluster_epoch(),
                                    route_map_validity: cluster.route_map_validity(),
                                });
                            status.last_error = None;
                        }
                        Err(error) => {
                            status.failures += 1;
                            status.last_error = Some(error.to_string());
                        }
                    }
                }

                if wait_for_runtime_map_worker(&worker_stop, refresh_interval) {
                    break;
                }
            })
            .map_err(|source| StorageClusterRuntimeMapRefreshError::RefreshLoopSpawn { source })?;

        let mut handles = vec![refresh_handle];
        if recover_pending_commands {
            let worker_stop = Arc::clone(&stop);
            let recovery_status = Arc::clone(&status);
            let worker_control_plane = Arc::clone(&control_plane);
            let worker_authority_now_ms = Arc::clone(&authority_now_ms);
            let recovery_route_handle = self;
            // Recovery deliberately uses a separate control-plane call path
            // and thread so a slow discovery or historical-route operation
            // cannot prevent the refresh worker from renewing request routes.
            let recovery_handle = thread::Builder::new()
                .name("argmin-storage-cluster-pending-command-recovery".to_string())
                .spawn(move || {
                    let mut fallback_schedule =
                        PendingMetadataCommandFallbackSchedule::default();
                    loop {
                        let discovery_now_ms = worker_authority_now_ms();
                        let request_generation =
                            recovery_request_generation.load(Ordering::Acquire);
                        let fallback_requested =
                            fallback_schedule.should_attempt(request_generation, Instant::now());
                        let targeted_recovery_result = match worker_control_plane
                            .pending_metadata_command_recoveries(discovery_now_ms)
                        {
                            Ok(listing)
                                if !listing.tasks().is_empty()
                                    || !listing.failures().is_empty() =>
                            {
                                recovery_route_handle
                                    .recover_authorized_pending_metadata_commands(
                                        worker_control_plane.as_ref(),
                                        worker_authority_now_ms.as_ref(),
                                        admission_settings,
                                        listing,
                                    )
                            }
                            Ok(_) => Ok(0),
                            Err(error) => Err(
                                PendingMetadataCommandRefreshRecoveryError::ControlPlane(error),
                            ),
                        };
                        let fallback_recovery_result = fallback_requested.then(|| {
                            let result = recovery_route_handle
                                .current()
                                .drain_pending_metadata_commands_for_current_map()
                                .map_err(PendingMetadataCommandRefreshRecoveryError::Recover);
                            fallback_schedule.record_attempt(
                                request_generation,
                                Instant::now(),
                                result.is_ok(),
                            );
                            let mut status = recovery_status
                                .lock()
                                .unwrap_or_else(|poisoned| poisoned.into_inner());
                            status.fallback_recovery_attempts += 1;
                            status.fallback_recovery_failures += u64::from(result.is_err());
                            result
                        });
                        let recovery_result = match (
                            targeted_recovery_result,
                            fallback_recovery_result,
                        ) {
                            (Err(error), _) | (Ok(_), Some(Err(error))) => Err(error),
                            (Ok(targeted), Some(Ok(fallback))) => Ok(targeted + fallback),
                            (Ok(targeted), None) => Ok(targeted),
                        };
                        if let Err(error) = &recovery_result {
                            let _ = observability::emit_flight_event(
                                "storage",
                                "pending_metadata_command_recovery_error",
                                format!("kind={}", error.diagnostic_kind()),
                            );
                            let _ = observability::event(
                                "storage",
                                "pending_metadata_command_recovery_error",
                                Some(format_args!("kind={}", error.diagnostic_kind())),
                            );
                        }

                        if wait_for_runtime_map_worker(&worker_stop, refresh_interval) {
                            break;
                        }
                    }
                });
            match recovery_handle {
                Ok(handle) => handles.push(handle),
                Err(source) => {
                    stop_runtime_map_workers(&stop);
                    for handle in handles {
                        let _ = handle.join();
                    }
                    return Err(StorageClusterRuntimeMapRefreshError::RefreshLoopSpawn {
                        source,
                    });
                }
            }
        }

        Ok(StorageClusterRuntimeMapRefreshLoop {
            stop,
            status,
            handles,
        })
    }

    fn recover_reported_pending_metadata_command(
        &self,
        control_plane: &impl ControlPlaneRuntimeMapSource,
        authority_now_ms: u64,
        admission_settings: Option<LocalUnixStorageNodeClientAdmissionSettings>,
        pg_id: PgId,
        reporting_node: NodeId,
        pending: PendingMetadataCommandObservation,
    ) -> Result<usize, PendingMetadataCommandRefreshRecoveryError> {
        let pg_runtime_map = control_plane.pg_runtime_map_snapshot(pg_id, authority_now_ms)?;
        let expected_recovery =
            crate::control_plane::PendingMetadataCommandRecovery::new(reporting_node, pending);
        let current_route = pg_runtime_map
            .pg_routes()
            .iter()
            .find(|route| route.pg_id() == pg_id)
            .ok_or(ControlPlaneError::UnknownPg { pg_id: pg_id.get() })?;
        let actual_recovery = current_route.pending_metadata_command_recovery();
        let current_active_recovery = current_route.state() == PgState::Active
            && pending.cluster_epoch() == pg_runtime_map.cluster_epoch()
            && current_route.primary_node_id() == reporting_node;
        let historical_peering_recovery = current_route.state() == PgState::Peering
            && pending.cluster_epoch() < pg_runtime_map.cluster_epoch();
        if actual_recovery != Some(expected_recovery)
            || (!current_active_recovery && !historical_peering_recovery)
        {
            return Err(
                PendingMetadataCommandRefreshRecoveryError::AuthorizationChanged {
                    pg_id: pg_id.get(),
                    expected: expected_recovery,
                    actual_state: current_route.state(),
                    actual: actual_recovery,
                },
            );
        }
        let historical_runtime_map = if historical_peering_recovery {
            let historical_route =
                pg_runtime_map.reconstructed_pg_route_at_epoch(pg_id, pending.cluster_epoch())?;
            if historical_route.state() != PgState::Active {
                return Err(
                    PendingMetadataCommandRefreshRecoveryError::HistoricalRouteNotActive {
                        pg_id: pg_id.get(),
                        pending_epoch: pending.cluster_epoch(),
                        state: historical_route.state(),
                    },
                );
            }
            if historical_route.primary_node_id() != reporting_node {
                return Err(
                    PendingMetadataCommandRefreshRecoveryError::ReportingNodeNotHistoricalPrimary {
                        pg_id: pg_id.get(),
                        pending_epoch: pending.cluster_epoch(),
                        reporting_node: reporting_node.as_u32(),
                        actual_primary: historical_route.primary_node_id().as_u32(),
                    },
                );
            }
            Some(pg_runtime_map.runtime_map_at_epoch(pending.cluster_epoch())?)
        } else {
            None
        };
        let recovery_runtime_map = historical_runtime_map.as_ref().unwrap_or(&pg_runtime_map);
        let current = self.current();
        let recovery_cluster = match admission_settings {
            Some(admission_settings) => current
                .historical_recovery_cluster_with_storage_rpc_clients(
                    recovery_runtime_map,
                    admission_settings,
                )?,
            None => {
                let local_map = LocalClusterMap::open_runtime_map_with_existing_local_nodes(
                    &current.local_map,
                    recovery_runtime_map,
                )?;
                StorageCluster::from_runtime_local_map(
                    Arc::new(local_map),
                    recovery_runtime_map,
                )?
            }
        };
        let primary = recovery_cluster
            .local_map
            .metadata_pg_primary_node_for_metadata_command_recovery(
                pending.cluster_epoch(),
                pg_id,
            )?;
        let Some(command) = primary
            .metadata_command_client()
            .pending_metadata_command_envelope(pg_id, pending.cluster_epoch())?
        else {
            return Ok(0);
        };
        let command_id = command.id();
        let observation_matches = |candidate: &MetadataCommandEnvelope| {
            let candidate_id = candidate.id();
            candidate_id.cluster_epoch() == pending.cluster_epoch()
                && candidate_id.pg_id() == pg_id
                && candidate_id.log_index().get() == pending.log_index()
                && candidate.checksum_crc64() == pending.command_checksum()
        };
        let authorized_source = if observation_matches(&command) {
            command.clone()
        } else if let Some(source) = current
            .local_map
            .runtime_state()
            .metadata_command_recovery_handoff_source(pg_id, &command)
            .filter(|source| {
                observation_matches(source)
                    && command.id().cluster_epoch() == source.id().cluster_epoch()
                    && command.id().log_index().get() > source.id().log_index().get()
                    && command
                        .payload()
                        .is_authorized_recovery_derivative_of(source.payload())
            })
        {
            source
        } else {
            return Err(
                PendingMetadataCommandRefreshRecoveryError::IdentityChanged {
                    pg_id: pg_id.get(),
                    expected_epoch: pending.cluster_epoch(),
                    expected_index: pending.log_index(),
                    expected_checksum: pending.command_checksum(),
                    actual_epoch: command_id.cluster_epoch(),
                    actual_index: command_id.log_index().get(),
                    actual_checksum: command.checksum_crc64(),
                },
            );
        };
        let outcome = recovery_cluster
            .drain_pending_metadata_command_with_authorized_recovery_source(
                pg_id,
                &command,
                &authorized_source,
                &current,
            )?;
        Ok(usize::from(outcome.is_terminal()))
    }

    fn recover_authorized_pending_metadata_commands<F>(
        &self,
        control_plane: &impl ControlPlaneRuntimeMapSource,
        authority_now_ms: &F,
        admission_settings: Option<LocalUnixStorageNodeClientAdmissionSettings>,
        listing: crate::control_plane::PendingMetadataCommandRecoveryListing,
    ) -> Result<usize, PendingMetadataCommandRefreshRecoveryError>
    where
        F: Fn() -> u64,
    {
        let mut recovered = 0;
        let mut first_error = None;
        let (recoveries, discovery_failures) = listing.into_parts();
        for task in recoveries {
            let recovery = task.recovery();
            match self.recover_reported_pending_metadata_command(
                control_plane,
                authority_now_ms(),
                admission_settings,
                task.pg_id(),
                recovery.reporting_node_id(),
                recovery.pending(),
            ) {
                Ok(count) => recovered += count,
                Err(error) if first_error.is_none() => first_error = Some(error),
                Err(_) => {}
            }
        }
        if first_error.is_none() {
            first_error = discovery_failures.into_iter().next().map(|failure| {
                PendingMetadataCommandRefreshRecoveryError::DiscoveryFailure {
                    pg_id: failure.pg_id().get(),
                    kind: failure.kind(),
                    detail: failure.detail().to_owned(),
                }
            });
        }
        if let Some(error) = first_error {
            return Err(error);
        }
        Ok(recovered)
    }
}

impl StorageClusterRuntimeMapHandle {
    /// Construct dynamic publication capability for an authoritative runtime
    /// map generation.
    pub fn new(initial: Arc<StorageCluster>) -> Result<Self, StorageClusterRuntimeMapRefreshError> {
        initial.route_authority.dynamic_proof()?;
        Ok(Self {
            route_handle: StorageClusterRouteHandle::from_authorized_cluster(initial),
        })
    }

    /// Return the opaque request-admission capability shared with coordinators
    /// and other consumers that must not publish route maps.
    pub fn route_handle(&self) -> StorageClusterRouteHandle {
        self.route_handle.clone()
    }

    pub fn current(&self) -> Arc<StorageCluster> {
        self.route_handle.current()
    }

    pub fn install(
        &self,
        candidate: Arc<StorageCluster>,
    ) -> Result<(), StorageClusterRuntimeMapRefreshError> {
        self.route_handle.install(candidate)
    }

    #[cfg(any(test, feature = "test-hooks"))]
    pub(crate) fn test_install_if_current(
        &self,
        expected_current: &Arc<StorageCluster>,
        candidate: Arc<StorageCluster>,
    ) -> Result<bool, StorageClusterRuntimeMapRefreshError> {
        self.route_handle
            .test_install_if_current(expected_current, candidate)
    }

    pub fn refresh_from_control_plane_runtime_map(
        &self,
        control_plane: &impl ControlPlaneRuntimeMapSource,
        authority_now_ms: u64,
    ) -> Result<Arc<StorageCluster>, StorageClusterRuntimeMapRefreshError> {
        self.route_handle
            .refresh_from_control_plane_runtime_map(control_plane, authority_now_ms)
    }

    pub fn refresh_from_control_plane_runtime_map_with_unix_storage_node_clients(
        &self,
        control_plane: &impl ControlPlaneRuntimeMapSource,
        authority_now_ms: u64,
        admission_settings: LocalUnixStorageNodeClientAdmissionSettings,
    ) -> Result<Arc<StorageCluster>, StorageClusterRuntimeMapRefreshError> {
        self.route_handle
            .refresh_from_control_plane_runtime_map_with_unix_storage_node_clients(
                control_plane,
                authority_now_ms,
                admission_settings,
            )
    }

    pub fn spawn_control_plane_refresh_loop<S, F>(
        self,
        control_plane: S,
        refresh_interval: Duration,
        authority_now_ms: F,
    ) -> Result<StorageClusterRuntimeMapRefreshLoop, StorageClusterRuntimeMapRefreshError>
    where
        S: ControlPlaneRuntimeMapSource + Send + Sync + 'static,
        F: Fn() -> u64 + Send + Sync + 'static,
    {
        self.route_handle.spawn_control_plane_refresh_loop(
            control_plane,
            refresh_interval,
            authority_now_ms,
        )
    }

    pub fn spawn_control_plane_refresh_loop_with_unix_storage_node_clients<S, F>(
        self,
        control_plane: S,
        refresh_interval: Duration,
        authority_now_ms: F,
        admission_settings: LocalUnixStorageNodeClientAdmissionSettings,
    ) -> Result<StorageClusterRuntimeMapRefreshLoop, StorageClusterRuntimeMapRefreshError>
    where
        S: ControlPlaneRuntimeMapSource + Send + Sync + 'static,
        F: Fn() -> u64 + Send + Sync + 'static,
    {
        self.route_handle
            .spawn_control_plane_refresh_loop_with_unix_storage_node_clients(
                control_plane,
                refresh_interval,
                authority_now_ms,
                admission_settings,
            )
    }

    pub fn spawn_control_plane_refresh_only_loop_with_unix_storage_node_clients<S, F>(
        self,
        control_plane: S,
        refresh_interval: Duration,
        authority_now_ms: F,
        admission_settings: LocalUnixStorageNodeClientAdmissionSettings,
    ) -> Result<StorageClusterRuntimeMapRefreshLoop, StorageClusterRuntimeMapRefreshError>
    where
        S: ControlPlaneRuntimeMapSource + Send + Sync + 'static,
        F: Fn() -> u64 + Send + Sync + 'static,
    {
        self.route_handle
            .spawn_control_plane_refresh_only_loop_with_unix_storage_node_clients(
                control_plane,
                refresh_interval,
                authority_now_ms,
                admission_settings,
            )
    }
}

fn runtime_map_refresh_error_requires_current_map_invalidation(
    error: &StorageClusterRuntimeMapRefreshError,
) -> bool {
    matches!(
        error,
        StorageClusterRuntimeMapRefreshError::ControlPlane(
            ControlPlaneError::PgPeeringPendingMetadataCommand { .. }
        )
    )
}
