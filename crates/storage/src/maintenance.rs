use std::sync::Mutex;

use crate::cluster::{
    PlacedSegmentShardBackfillCandidateEnqueueSummary,
    PlacedSegmentShardBackfillCandidateScanCursor, StorageClusterRouteHandle,
};
use crate::StoreError;

const TRACE_TARGET: &str = "storage";

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
