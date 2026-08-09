// Copyright The Argmin Authors.
// SPDX-License-Identifier: Apache-2.0

#[derive(Clone, Default)]
struct StorageNodeMetadataCommandLocks {
    state: Arc<StorageNodeMetadataCommandLockState>,
}
const METADATA_COMMAND_LOCK_WAIT_DIAGNOSTIC_AFTER: Duration = Duration::from_millis(250);
const METADATA_COMMAND_LOCK_WAIT_DIAGNOSTIC_INTERVAL: Duration = Duration::from_millis(250);
const METADATA_COMMAND_LOCK_WAIT_POLL_INTERVAL: Duration = Duration::from_millis(50);
const METADATA_COMMAND_LOCK_WAIT_TIMEOUT: Duration = Duration::from_millis(500);

#[derive(Clone, Copy, Debug)]
struct StorageNodeMetadataCommandLockContext {
    request_id: u64,
    kind: StorageRpcMessageKind,
}

#[derive(Clone, Copy, Debug)]
struct StorageNodeMetadataCommandLockHolder {
    acquired_context: Option<StorageNodeMetadataCommandLockContext>,
    current_context: Option<StorageNodeMetadataCommandLockContext>,
    acquired_at: Instant,
    current_started_at: Option<Instant>,
}

impl StorageNodeMetadataCommandLocks {
    #[cfg(test)]
    fn set_before_wait_hook(&self, hook: MetadataCommandBeforeWaitHook) {
        *self
            .state
            .before_wait_hook
            .lock()
            .unwrap_or_else(|e| e.into_inner()) = Some(hook);
    }

    #[cfg(test)]
    fn suppress_lock_wait_stderr(&self) -> SuppressMetadataCommandLockWaitStderr {
        let previous = self
            .state
            .suppress_lock_wait_stderr
            .swap(true, std::sync::atomic::Ordering::SeqCst);
        SuppressMetadataCommandLockWaitStderr {
            locks: self.clone(),
            previous,
        }
    }

    #[cfg(test)]
    fn lock_wait_stderr_suppressed(&self) -> bool {
        self.state
            .suppress_lock_wait_stderr
            .load(std::sync::atomic::Ordering::SeqCst)
    }

    #[cfg(not(test))]
    fn lock_wait_stderr_suppressed(&self) -> bool {
        false
    }

    fn acquire(
        &self,
        node_id: NodeId,
        pg_id: PgId,
        context: Option<StorageNodeMetadataCommandLockContext>,
    ) -> Result<StorageNodeMetadataCommandGuard, StorageRpcErrorResponse> {
        self.acquire_with_timeout(node_id, pg_id, context, METADATA_COMMAND_LOCK_WAIT_TIMEOUT)
    }

    fn acquire_with_timeout(
        &self,
        node_id: NodeId,
        pg_id: PgId,
        context: Option<StorageNodeMetadataCommandLockContext>,
        wait_timeout: Duration,
    ) -> Result<StorageNodeMetadataCommandGuard, StorageRpcErrorResponse> {
        let started_at = Instant::now();
        let mut next_diagnostic_at = started_at + METADATA_COMMAND_LOCK_WAIT_DIAGNOSTIC_AFTER;
        let mut waited = false;
        let mut held = self.state.held.lock().unwrap_or_else(|e| e.into_inner());
        while let Some(holder) = held.get(&pg_id).copied() {
            waited = true;
            #[cfg(test)]
            let before_wait_hook = self
                .state
                .before_wait_hook
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .clone();
            let now = Instant::now();
            let waited_for = now.saturating_duration_since(started_at);
            if waited_for >= wait_timeout {
                drop(held);
                emit_metadata_command_lock_wait_diagnostic(
                    node_id,
                    pg_id,
                    context,
                    holder,
                    waited_for,
                    self.lock_wait_stderr_suppressed(),
                );
                return Err(StorageRpcErrorResponse {
                    code: StorageRpcErrorCode::MetadataCommandContention,
                    message: format!(
                        "metadata command lock wait for PG {} exceeded {}ms",
                        pg_id.get(),
                        wait_timeout.as_millis()
                    ),
                });
            }
            if now >= next_diagnostic_at {
                next_diagnostic_at = now + METADATA_COMMAND_LOCK_WAIT_DIAGNOSTIC_INTERVAL;
                drop(held);
                emit_metadata_command_lock_wait_diagnostic(
                    node_id,
                    pg_id,
                    context,
                    holder,
                    waited_for,
                    self.lock_wait_stderr_suppressed(),
                );
                held = self.state.held.lock().unwrap_or_else(|e| e.into_inner());
                continue;
            }
            drop(held);
            #[cfg(test)]
            if let Some(hook) = before_wait_hook {
                hook(pg_id);
            }
            held = self.state.held.lock().unwrap_or_else(|e| e.into_inner());
            if !held.contains_key(&pg_id) {
                continue;
            }
            let remaining = wait_timeout
                .checked_sub(Instant::now().saturating_duration_since(started_at))
                .unwrap_or_default();
            if remaining.is_zero() {
                continue;
            }
            let wait_interval = METADATA_COMMAND_LOCK_WAIT_POLL_INTERVAL.min(remaining);
            let (next_held, _) = self
                .state
                .available
                .wait_timeout(held, wait_interval)
                .unwrap_or_else(|e| e.into_inner());
            held = next_held;
        }
        if waited {
            let _ = observability::emit_metadata_command_session_wait(
                "storage",
                observability::MetadataCommandSessionWaitSummary {
                    node_id: node_id.as_u32(),
                    pg_id: pg_id.get(),
                    wait_us: started_at.elapsed().as_micros(),
                },
            );
        }
        held.insert(
            pg_id,
            StorageNodeMetadataCommandLockHolder {
                acquired_context: context,
                current_context: context,
                acquired_at: Instant::now(),
                current_started_at: Some(Instant::now()),
            },
        );
        Ok(StorageNodeMetadataCommandGuard {
            locks: self.clone(),
            pg_id,
            released: false,
        })
    }

    fn update_context(&self, pg_id: PgId, context: Option<StorageNodeMetadataCommandLockContext>) {
        let mut held = self.state.held.lock().unwrap_or_else(|e| e.into_inner());
        if let Some(holder) = held.get_mut(&pg_id) {
            holder.current_context = context;
            holder.current_started_at = context.map(|_| Instant::now());
        }
    }

    fn release(&self, pg_id: PgId) {
        let mut held = self.state.held.lock().unwrap_or_else(|e| e.into_inner());
        if held.remove(&pg_id).is_some() {
            self.state.available.notify_all();
        }
    }
}

fn emit_metadata_command_lock_wait_diagnostic(
    node_id: NodeId,
    pg_id: PgId,
    waiter: Option<StorageNodeMetadataCommandLockContext>,
    holder: StorageNodeMetadataCommandLockHolder,
    waited: Duration,
    suppress_stderr: bool,
) {
    let waiter_request_id = waiter
        .map(|context| context.request_id.to_string())
        .unwrap_or_else(|| "unknown".to_string());
    let waiter_kind = waiter
        .map(|context| context.kind.operation_name())
        .unwrap_or("unknown");
    let holder_request_id = holder
        .acquired_context
        .map(|context| context.request_id.to_string())
        .unwrap_or_else(|| "unknown".to_string());
    let holder_kind = holder
        .acquired_context
        .map(|context| context.kind.operation_name())
        .unwrap_or("unknown");
    let held_us = holder.acquired_at.elapsed().as_micros();
    let (holder_current_request_id, holder_current_kind, holder_current_elapsed_us) =
        match (holder.current_context, holder.current_started_at) {
            (Some(context), Some(started_at)) => (
                context.request_id.to_string(),
                context.kind.operation_name(),
                started_at.elapsed().as_micros().to_string(),
            ),
            _ => ("none".to_string(), "none", "none".to_string()),
        };
    let waited_us = waited.as_micros();
    let detail = format!(
        "node_id={} pg_id={} waiter_request_id={} waiter_kind=\"{}\" waited_us={} holder_request_id={} holder_kind=\"{}\" holder_held_us={} holder_current_request_id={} holder_current_kind=\"{}\" holder_current_elapsed_us={}",
        node_id.as_u32(),
        pg_id.get(),
        waiter_request_id,
        waiter_kind,
        waited_us,
        holder_request_id,
        holder_kind,
        held_us,
        holder_current_request_id,
        holder_current_kind,
        holder_current_elapsed_us
    );
    let _ = observability::emit_flight_event(
        "storage",
        "metadata_command_lock_wait_blocked",
        detail.clone(),
    );
    if !suppress_stderr {
        eprintln!("metadata_command_lock_wait_blocked {detail}");
    }
}

#[derive(Default)]
struct StorageNodeMetadataCommandLockState {
    held: Mutex<BTreeMap<PgId, StorageNodeMetadataCommandLockHolder>>,
    available: Condvar,
    #[cfg(test)]
    before_wait_hook: Mutex<Option<MetadataCommandBeforeWaitHook>>,
    #[cfg(test)]
    suppress_lock_wait_stderr: std::sync::atomic::AtomicBool,
}

#[cfg(test)]
pub(crate) struct SuppressMetadataCommandLockWaitStderr {
    locks: StorageNodeMetadataCommandLocks,
    previous: bool,
}

#[cfg(test)]
impl Drop for SuppressMetadataCommandLockWaitStderr {
    fn drop(&mut self) {
        self.locks
            .state
            .suppress_lock_wait_stderr
            .store(self.previous, std::sync::atomic::Ordering::SeqCst);
    }
}

struct StorageNodeMetadataCommandGuard {
    locks: StorageNodeMetadataCommandLocks,
    pg_id: PgId,
    released: bool,
}

impl StorageNodeMetadataCommandGuard {
    fn release(&mut self) {
        if !self.released {
            self.locks.release(self.pg_id);
            self.released = true;
        }
    }
}

impl Drop for StorageNodeMetadataCommandGuard {
    fn drop(&mut self) {
        self.release();
    }
}

#[derive(Clone, Default)]
struct StorageNodeRouteAdmissionGate {
    inner: Arc<StorageNodeRouteAdmissionGateInner>,
}

#[derive(Default)]
struct StorageNodeRouteAdmissionGateInner {
    state: Mutex<StorageNodeRouteAdmissionState>,
    changed: Condvar,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
enum StorageNodeRouteTransitionState {
    #[default]
    Open,
    Draining,
    Publishing,
}

#[derive(Debug, Default)]
struct StorageNodeRouteAdmissionState {
    active_frames: usize,
    transition: StorageNodeRouteTransitionState,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum StorageNodeRouteAdmissionClass {
    Active,
    RetainedCleanup,
}

impl StorageNodeRouteAdmissionGate {
    fn acquire(&self, class: StorageNodeRouteAdmissionClass) -> StorageNodeRouteAdmissionPermit {
        let mut state = self
            .inner
            .state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        while state.transition != StorageNodeRouteTransitionState::Open
            && !(class == StorageNodeRouteAdmissionClass::RetainedCleanup
                && state.transition == StorageNodeRouteTransitionState::Draining)
        {
            state = self
                .inner
                .changed
                .wait(state)
                .unwrap_or_else(|poisoned| poisoned.into_inner());
        }
        state.active_frames += 1;
        StorageNodeRouteAdmissionPermit {
            gate: self.clone(),
            class,
        }
    }

    fn begin_transition(&self) -> StorageNodeRouteTransitionGuard {
        let mut state = self
            .inner
            .state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        while state.transition != StorageNodeRouteTransitionState::Open {
            state = self
                .inner
                .changed
                .wait(state)
                .unwrap_or_else(|poisoned| poisoned.into_inner());
        }
        state.transition = StorageNodeRouteTransitionState::Draining;
        self.inner.changed.notify_all();
        while state.active_frames != 0 {
            state = self
                .inner
                .changed
                .wait(state)
                .unwrap_or_else(|poisoned| poisoned.into_inner());
        }
        state.transition = StorageNodeRouteTransitionState::Publishing;
        StorageNodeRouteTransitionGuard { gate: self.clone() }
    }
}

struct StorageNodeRouteAdmissionPermit {
    gate: StorageNodeRouteAdmissionGate,
    class: StorageNodeRouteAdmissionClass,
}

impl Drop for StorageNodeRouteAdmissionPermit {
    fn drop(&mut self) {
        let mut state = self
            .gate
            .inner
            .state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        state.active_frames = state
            .active_frames
            .checked_sub(1)
            .expect("route admission permit count must not underflow");
        self.gate.inner.changed.notify_all();
    }
}

struct StorageNodeRouteTransitionGuard {
    gate: StorageNodeRouteAdmissionGate,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct StorageNodeRouteFence {
    cluster_epoch: ClusterEpoch,
    valid_until_ms: Option<u64>,
    local_valid_until_monotonic_ms: Option<u64>,
}

impl StorageNodeRouteFence {
    fn current(
        config: &StorageNodeProcessConfig,
        route_map_lease: Option<BoundRouteMapLease>,
    ) -> Self {
        Self {
            cluster_epoch: config.cluster_epoch,
            valid_until_ms: config.route_map_valid_until_ms(),
            local_valid_until_monotonic_ms: route_map_lease
                .map(BoundRouteMapLease::local_valid_until_monotonic_ms),
        }
    }

    fn historical(
        cluster_epoch: ClusterEpoch,
        valid_until_ms: u64,
        local_valid_until_monotonic_ms: u64,
    ) -> Self {
        Self {
            cluster_epoch,
            valid_until_ms: Some(valid_until_ms),
            local_valid_until_monotonic_ms: Some(local_valid_until_monotonic_ms),
        }
    }

    fn validate_rpc_at(
        self,
        now_ms: u64,
        local_monotonic_ms: u64,
    ) -> Result<(), StorageRpcErrorResponse> {
        let Some(valid_until_ms) = self.valid_until_ms else {
            return Ok(());
        };
        if self.local_valid_until_monotonic_ms.is_some()
            && validate_process_lease_clock(
                now_ms,
                crate::clock::clock_health_time_millis(),
                CONTROL_PLANE_CLOCK_SKEW_BUDGET_MS,
            )
            .is_ok()
            && self
                .local_valid_until_monotonic_ms
                .is_some_and(|deadline| deadline > local_monotonic_ms)
        {
            return Ok(());
        }
        Err(StorageRpcErrorResponse {
            code: StorageRpcErrorCode::StaleShardLocation,
            message: format!(
                "storage-node route for cluster epoch {} expired at {valid_until_ms}ms, now {now_ms}ms",
                self.cluster_epoch.get()
            ),
        })
    }

    fn validate_store_at(self, now_ms: u64, local_monotonic_ms: u64) -> Result<(), StoreError> {
        let Some(valid_until_ms) = self.valid_until_ms else {
            return Ok(());
        };
        if self.local_valid_until_monotonic_ms.is_some()
            && validate_process_lease_clock(
                now_ms,
                crate::clock::clock_health_time_millis(),
                CONTROL_PLANE_CLOCK_SKEW_BUDGET_MS,
            )
            .is_ok()
            && self
                .local_valid_until_monotonic_ms
                .is_some_and(|deadline| deadline > local_monotonic_ms)
        {
            return Ok(());
        }
        Err(StoreError::RouteMapExpired {
            cluster_epoch: self.cluster_epoch,
            valid_until_ms,
            now_ms,
        })
    }

    fn intersect_effect_fence(
        self,
        delegated: AdmittedRouteEffectFence,
    ) -> Result<AdmittedRouteEffectFence, StoreError> {
        let local = match (self.valid_until_ms, self.local_valid_until_monotonic_ms) {
            (None, None) => AdmittedRouteEffectFence::unbounded(self.cluster_epoch),
            (Some(valid_until_ms), Some(local_valid_until_monotonic_ms)) => {
                AdmittedRouteEffectFence::bounded(
                    self.cluster_epoch,
                    valid_until_ms,
                    local_valid_until_monotonic_ms,
                )
            }
            _ => {
                return Err(StoreError::RouteMapExpired {
                    cluster_epoch: self.cluster_epoch,
                    valid_until_ms: self.valid_until_ms.unwrap_or_default(),
                    now_ms: crate::clock::current_time_millis(),
                });
            }
        };
        local.intersect(delegated)
    }
}

impl Drop for StorageNodeRouteTransitionGuard {
    fn drop(&mut self) {
        let mut state = self
            .gate
            .inner
            .state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        state.transition = StorageNodeRouteTransitionState::Open;
        self.gate.inner.changed.notify_all();
    }
}
