use std::collections::HashMap;
use std::fmt;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Condvar, Mutex, OnceLock, Weak};
use std::thread::JoinHandle;
use std::time::Duration;

use crate::cluster::{
    PlacedSegmentShardBackfillCandidateEnqueueSummary,
    PlacedSegmentShardBackfillCandidateScanCursor, StorageClusterRouteHandle,
    StreamSessionSweepSummary,
};
use crate::StoreError;

const TRACE_TARGET: &str = "storage";
const STREAM_SESSION_SWEEP_INTERVAL: Duration = Duration::from_secs(10);
const STREAM_SESSION_SCAVENGE_MAX_AGE_MILLIS: u64 = 60_000;

static STREAM_SESSION_SWEEPER_REGISTRY: OnceLock<
    Mutex<HashMap<crate::ProcessLocalRegistryKey, Weak<StorageStreamSessionSweeper>>>,
> = OnceLock::new();

/// Opaque failure to start a storage-owned maintenance worker.
pub struct StorageMaintenanceStartError {
    _diagnostic: Box<str>,
}

impl StorageMaintenanceStartError {
    fn worker_spawn(worker: &'static str, error: std::io::Error) -> Self {
        let diagnostic = format!("failed to start {worker}: {error}").into_boxed_str();
        let _ = observability::event(
            TRACE_TARGET,
            "storage_maintenance_worker_start_error",
            Some(format_args!("worker={worker} error={error}")),
        );
        Self {
            _diagnostic: diagnostic,
        }
    }
}

impl fmt::Debug for StorageMaintenanceStartError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("StorageMaintenanceStartError")
            .field("diagnostic", &"<redacted>")
            .finish()
    }
}

impl fmt::Display for StorageMaintenanceStartError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("failed to start storage maintenance worker")
    }
}

impl std::error::Error for StorageMaintenanceStartError {}

/// Opaque storage-owned state for bounded backfill-candidate discovery.
///
/// The cursor is deliberately retained with the route handle so callers can
/// request a scan without learning or persisting placement-group scan state.
pub struct StorageBackfillCandidateScanner {
    storage_handle: StorageClusterRouteHandle,
    cursor: Mutex<PlacedSegmentShardBackfillCandidateScanCursor>,
}

impl StorageBackfillCandidateScanner {
    #[must_use]
    pub fn new(storage_handle: StorageClusterRouteHandle) -> Self {
        Self {
            storage_handle,
            cursor: Mutex::new(PlacedSegmentShardBackfillCandidateScanCursor::default()),
        }
    }

    /// Run one bounded candidate scan and emit storage-owned diagnostics.
    ///
    /// Candidate counts, cursor state, and implementation failures remain
    /// inside storage; the caller merely schedules the maintenance operation.
    pub fn scan(&self) {
        match self.scan_inner() {
            Ok(summary) => emit_scan_summary(summary),
            Err(error) => {
                let _ =
                    observability::emit_shard_backfill_candidate_scan_error(TRACE_TARGET, &error);
            }
        }
    }

    fn scan_inner(&self) -> Result<PlacedSegmentShardBackfillCandidateEnqueueSummary, StoreError> {
        let mut cursor = self
            .cursor
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        self.storage_handle
            .current()
            .enqueue_placed_segment_shard_backfills_from_scavenger_references(&mut cursor)
    }

    #[cfg(test)]
    pub(crate) fn scan_with_limit(
        &self,
        scan_limit: usize,
    ) -> Result<PlacedSegmentShardBackfillCandidateEnqueueSummary, StoreError> {
        let mut cursor = self
            .cursor
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        self.storage_handle
            .current()
            .enqueue_placed_segment_shard_backfills_from_scavenger_references_with_cursor_and_limit(
                &mut cursor,
                scan_limit,
            )
    }
}

fn emit_scan_summary(summary: PlacedSegmentShardBackfillCandidateEnqueueSummary) {
    let _ = observability::emit_shard_backfill_candidate_scan(
        TRACE_TARGET,
        observability::ShardBackfillCandidateScanSummary {
            scanned: summary.scanned,
            current_epoch: summary.current_epoch,
            already_queued: summary.already_queued,
            already_complete: summary.already_complete,
            enqueued: summary.enqueued,
            unrecoverable: summary.unrecoverable,
            deferred: summary.deferred,
            failed: summary.failed,
            limit_reached: summary.limit_reached,
        },
    );
}

/// Opaque storage-owned abandoned stream-session cleanup worker.
pub struct StorageStreamSessionSweeper {
    storage_handle: StorageClusterRouteHandle,
    #[cfg(feature = "test-hooks")]
    max_age_ms: u64,
    stop: Arc<AtomicBool>,
    wake: Arc<(Mutex<bool>, Condvar)>,
    handle: Mutex<Option<JoinHandle<()>>>,
}

#[cfg(feature = "test-hooks")]
#[doc(hidden)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct StorageStreamSessionSweepTestSummary {
    pub discovered: usize,
    pub due: usize,
    pub cleaned: usize,
    pub reservation_check_failed: usize,
    pub abort_failed: usize,
}

impl StorageStreamSessionSweeper {
    pub fn acquire_shared(
        storage_handle: &StorageClusterRouteHandle,
    ) -> Result<Arc<Self>, StorageMaintenanceStartError> {
        Self::acquire_shared_with_settings(
            storage_handle,
            STREAM_SESSION_SWEEP_INTERVAL,
            STREAM_SESSION_SCAVENGE_MAX_AGE_MILLIS,
        )
    }

    fn acquire_shared_with_settings(
        storage_handle: &StorageClusterRouteHandle,
        sweep_interval: Duration,
        max_age_ms: u64,
    ) -> Result<Arc<Self>, StorageMaintenanceStartError> {
        let registry = STREAM_SESSION_SWEEPER_REGISTRY.get_or_init(|| Mutex::new(HashMap::new()));
        let mut registry = registry.lock().unwrap_or_else(|error| error.into_inner());
        registry.retain(|_, sweeper| sweeper.upgrade().is_some());

        let key = storage_handle.current().process_local_registry_key();
        if let Some(existing) = registry.get(&key).and_then(Weak::upgrade) {
            return Ok(existing);
        }
        if let Some(existing) = registry.values().filter_map(Weak::upgrade).find(|sweeper| {
            sweeper
                .storage_handle
                .shares_route_admission_with(storage_handle)
        }) {
            registry.insert(key, Arc::downgrade(&existing));
            return Ok(existing);
        }

        let sweeper = Self::spawn(storage_handle.clone(), sweep_interval, max_age_ms)?;
        registry.insert(key, Arc::downgrade(&sweeper));
        Ok(sweeper)
    }

    fn spawn(
        storage_handle: StorageClusterRouteHandle,
        sweep_interval: Duration,
        max_age_ms: u64,
    ) -> Result<Arc<Self>, StorageMaintenanceStartError> {
        let stop = Arc::new(AtomicBool::new(false));
        let wake = Arc::new((Mutex::new(false), Condvar::new()));
        let sweeper = Arc::new(Self {
            storage_handle: storage_handle.clone(),
            #[cfg(feature = "test-hooks")]
            max_age_ms,
            stop: Arc::clone(&stop),
            wake: Arc::clone(&wake),
            handle: Mutex::new(None),
        });
        let handle = std::thread::Builder::new()
            .name("argmin-stream-session-sweeper".to_string())
            .spawn(move || {
                while !stop.load(Ordering::SeqCst) {
                    sweep_abandoned_stream_sessions(&storage_handle, max_age_ms);
                    if stop.load(Ordering::SeqCst) {
                        break;
                    }
                    let stop_guard = wake.0.lock().unwrap_or_else(|error| error.into_inner());
                    if *stop_guard {
                        break;
                    }
                    let _ = wake
                        .1
                        .wait_timeout_while(stop_guard, sweep_interval, |stop_requested| {
                            !*stop_requested
                        })
                        .unwrap_or_else(|error| error.into_inner());
                }
            })
            .map_err(|error| {
                StorageMaintenanceStartError::worker_spawn("stream-session sweeper", error)
            })?;
        *sweeper
            .handle
            .lock()
            .unwrap_or_else(|error| error.into_inner()) = Some(handle);
        Ok(sweeper)
    }

    #[must_use]
    pub fn disabled(storage_handle: StorageClusterRouteHandle) -> Arc<Self> {
        Arc::new(Self {
            storage_handle,
            #[cfg(feature = "test-hooks")]
            max_age_ms: STREAM_SESSION_SCAVENGE_MAX_AGE_MILLIS,
            stop: Arc::new(AtomicBool::new(true)),
            wake: Arc::new((Mutex::new(true), Condvar::new())),
            handle: Mutex::new(None),
        })
    }

    #[cfg(feature = "test-hooks")]
    #[doc(hidden)]
    pub fn test_sweep_once(&self) -> StorageStreamSessionSweepTestSummary {
        let summary = sweep_abandoned_stream_sessions(&self.storage_handle, self.max_age_ms);
        StorageStreamSessionSweepTestSummary {
            discovered: summary.discovered,
            due: summary.due,
            cleaned: summary.cleaned,
            reservation_check_failed: summary.reservation_check_failed,
            abort_failed: summary.abort_failed,
        }
    }

    #[cfg(feature = "test-hooks")]
    #[doc(hidden)]
    #[must_use]
    pub fn test_is_enabled(&self) -> bool {
        !self.stop.load(Ordering::SeqCst)
    }
}

impl Drop for StorageStreamSessionSweeper {
    fn drop(&mut self) {
        self.stop.store(true, Ordering::SeqCst);
        *self
            .wake
            .0
            .lock()
            .unwrap_or_else(|error| error.into_inner()) = true;
        self.wake.1.notify_all();
        if let Some(handle) = self
            .handle
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .take()
        {
            let _ = handle.join();
        }
    }
}

fn sweep_abandoned_stream_sessions(
    storage_handle: &StorageClusterRouteHandle,
    max_age_ms: u64,
) -> StreamSessionSweepSummary {
    let summary = storage_handle
        .current()
        .scavenge_abandoned_stream_sessions(max_age_ms);
    if summary.cleaned > 0 {
        let _ = observability::event(
            TRACE_TARGET,
            "stream_session_sweep_abandoned",
            Some(format_args!("aborted_sessions={}", summary.cleaned)),
        );
    }
    if summary.reservation_check_failed > 0 || summary.abort_failed > 0 {
        let _ = observability::event(
            TRACE_TARGET,
            "stream_session_sweep_error",
            Some(format_args!(
                "reservation_check_failed={} abort_failed={}",
                summary.reservation_check_failed, summary.abort_failed
            )),
        );
    }
    summary
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn maintenance_start_error_keeps_worker_diagnostic_opaque() {
        let error = StorageMaintenanceStartError {
            _diagnostic: "sensitive worker spawn detail".into(),
        };

        assert_eq!(
            error.to_string(),
            "failed to start storage maintenance worker"
        );
        assert_eq!(
            format!("{error:?}"),
            "StorageMaintenanceStartError { diagnostic: \"<redacted>\" }"
        );
        assert!(std::error::Error::source(&error).is_none());
    }
}
