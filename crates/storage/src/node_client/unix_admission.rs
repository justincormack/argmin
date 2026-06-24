use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Condvar, Mutex, OnceLock, Weak};
use std::time::{Duration, Instant};

use placement::NodeId;

use crate::storage_rpc::StorageRpcMessageKind;

pub(crate) const UNIX_STORAGE_NODE_DEFAULT_RPC_ADMISSION_LIMIT: usize = 1024;
pub(crate) const UNIX_STORAGE_NODE_MIN_RPC_ADMISSION_LIMIT: usize = 8;
pub(crate) const UNIX_STORAGE_NODE_DEFAULT_RPC_ADMISSION_WAIT_TIMEOUT: Duration =
    Duration::from_millis(250);
pub(crate) const UNIX_STORAGE_NODE_DEFAULT_RPC_CONTROL_ADMISSION_WAIT_TIMEOUT: Duration =
    Duration::from_secs(1);

pub(crate) struct UnixStorageNodeRpcAdmission {
    pub(crate) limit: usize,
    non_reserved_limit: usize,
    completion_limit: usize,
    pending_envelope_limit: usize,
    read_limit: usize,
    list_limit: usize,
    start_write_limit: usize,
    start_write_floor: usize,
    wait_timeout: Duration,
    control_wait_timeout: Duration,
    active: Mutex<UnixStorageNodeRpcAdmissionActive>,
    capacity_available: Condvar,
}

pub(crate) struct UnixStorageNodeRpcAdmissionPermit {
    admission: Arc<UnixStorageNodeRpcAdmission>,
    pub(crate) class: UnixStorageNodeRpcAdmissionClass,
    pending_envelope: bool,
    observed_active: bool,
}

pub(crate) enum UnixStorageNodeRpcAdmissionAcquire {
    Acquired {
        permit: UnixStorageNodeRpcAdmissionPermit,
        wait_us: u128,
    },
    TimedOut {
        wait_us: u128,
    },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum UnixStorageNodeRpcAdmissionClass {
    Control,
    Completion,
    Progress,
    StartWrite,
    Read,
    List,
}

impl UnixStorageNodeRpcAdmissionClass {
    pub(crate) fn as_str(self) -> &'static str {
        match self {
            Self::Control => "control",
            Self::Completion => "completion",
            Self::Progress => "progress",
            Self::StartWrite => "start_write",
            Self::Read => "read",
            Self::List => "list",
        }
    }
}

#[derive(Default)]
struct UnixStorageNodeRpcAdmissionActive {
    total: usize,
    completion: usize,
    pending_envelope: usize,
    progress: usize,
    start_write: usize,
    read: usize,
    list: usize,
}

impl UnixStorageNodeRpcAdmissionActive {
    fn non_reserved(&self) -> usize {
        self.progress + self.start_write + self.read + self.list
    }

    fn acquire(&mut self, class: UnixStorageNodeRpcAdmissionClass, pending_envelope: bool) {
        self.total += 1;
        match class {
            UnixStorageNodeRpcAdmissionClass::Control => {}
            UnixStorageNodeRpcAdmissionClass::Completion => {
                self.completion += 1;
                if pending_envelope {
                    self.pending_envelope += 1;
                }
            }
            UnixStorageNodeRpcAdmissionClass::Progress => self.progress += 1,
            UnixStorageNodeRpcAdmissionClass::StartWrite => self.start_write += 1,
            UnixStorageNodeRpcAdmissionClass::Read => self.read += 1,
            UnixStorageNodeRpcAdmissionClass::List => self.list += 1,
        }
    }

    fn release(&mut self, class: UnixStorageNodeRpcAdmissionClass, pending_envelope: bool) {
        self.total = self
            .total
            .checked_sub(1)
            .expect("Unix storage-node RPC admission release without acquire");
        match class {
            UnixStorageNodeRpcAdmissionClass::Control => {}
            UnixStorageNodeRpcAdmissionClass::Completion => {
                self.completion = self
                    .completion
                    .checked_sub(1)
                    .expect("Unix storage-node completion RPC admission release without acquire");
                if pending_envelope {
                    self.pending_envelope = self.pending_envelope.checked_sub(1).expect(
                        "Unix storage-node pending-envelope RPC admission release without acquire",
                    );
                }
            }
            UnixStorageNodeRpcAdmissionClass::Progress => {
                self.progress = self
                    .progress
                    .checked_sub(1)
                    .expect("Unix storage-node progress RPC admission release without acquire");
            }
            UnixStorageNodeRpcAdmissionClass::StartWrite => {
                self.start_write = self
                    .start_write
                    .checked_sub(1)
                    .expect("Unix storage-node start-write RPC admission release without acquire");
            }
            UnixStorageNodeRpcAdmissionClass::Read => {
                self.read = self
                    .read
                    .checked_sub(1)
                    .expect("Unix storage-node read RPC admission release without acquire");
            }
            UnixStorageNodeRpcAdmissionClass::List => {
                self.list = self
                    .list
                    .checked_sub(1)
                    .expect("Unix storage-node list RPC admission release without acquire");
            }
        }
    }
}

type UnixStorageNodeRpcAdmissionKey = (u32, PathBuf);
type UnixStorageNodeRpcAdmissionRegistry =
    Mutex<BTreeMap<UnixStorageNodeRpcAdmissionKey, Weak<UnixStorageNodeRpcAdmission>>>;

impl UnixStorageNodeRpcAdmission {
    #[cfg(test)]
    pub(crate) fn new(limit: usize) -> Self {
        Self::new_with_wait_timeout(
            limit,
            UNIX_STORAGE_NODE_DEFAULT_RPC_ADMISSION_WAIT_TIMEOUT,
            UNIX_STORAGE_NODE_DEFAULT_RPC_CONTROL_ADMISSION_WAIT_TIMEOUT,
        )
    }

    pub(crate) fn new_with_wait_timeout(
        limit: usize,
        wait_timeout: Duration,
        control_wait_timeout: Duration,
    ) -> Self {
        assert!(
            limit > 0,
            "Unix storage-node RPC admission limit must be > 0"
        );
        let reserved_control = (limit / 4).clamp(1, 4).min(limit);
        let shared_limit = limit.saturating_sub(reserved_control).max(1).min(limit);
        let list_limit = (shared_limit / 2).max(1).min(shared_limit);
        let start_write_floor = (limit / 8).clamp(1, 4).min(shared_limit);
        let completion_limit = limit.saturating_sub(start_write_floor).max(1);
        let pending_envelope_limit = (completion_limit / 2).max(1);
        Self {
            limit,
            non_reserved_limit: shared_limit,
            completion_limit,
            pending_envelope_limit,
            read_limit: shared_limit,
            list_limit,
            start_write_limit: shared_limit,
            start_write_floor,
            wait_timeout,
            control_wait_timeout,
            active: Mutex::new(UnixStorageNodeRpcAdmissionActive::default()),
            capacity_available: Condvar::new(),
        }
    }

    #[cfg(test)]
    pub(crate) fn try_acquire_for_test(
        self: &Arc<Self>,
    ) -> Option<UnixStorageNodeRpcAdmissionPermit> {
        let mut active = self.active.lock().unwrap_or_else(|e| e.into_inner());
        if active.total >= self.limit {
            return None;
        }
        active.acquire(UnixStorageNodeRpcAdmissionClass::Control, false);
        observability::storage_rpc_admission_class_acquired(
            UnixStorageNodeRpcAdmissionClass::Control.as_str(),
        );
        Some(UnixStorageNodeRpcAdmissionPermit {
            admission: Arc::clone(self),
            class: UnixStorageNodeRpcAdmissionClass::Control,
            pending_envelope: false,
            observed_active: true,
        })
    }

    #[cfg(test)]
    fn acquire(
        self: &Arc<Self>,
        class: UnixStorageNodeRpcAdmissionClass,
    ) -> UnixStorageNodeRpcAdmissionAcquire {
        self.acquire_with_pending_envelope(class, false)
    }

    pub(crate) fn acquire_with_kind(
        self: &Arc<Self>,
        class: UnixStorageNodeRpcAdmissionClass,
        kind: StorageRpcMessageKind,
    ) -> UnixStorageNodeRpcAdmissionAcquire {
        self.acquire_with_pending_envelope(
            class,
            kind == StorageRpcMessageKind::MetadataCommandPendingEnvelope,
        )
    }

    fn acquire_with_pending_envelope(
        self: &Arc<Self>,
        class: UnixStorageNodeRpcAdmissionClass,
        pending_envelope: bool,
    ) -> UnixStorageNodeRpcAdmissionAcquire {
        let started_at = Instant::now();
        let wait_timeout = self.wait_timeout_for_class(class);
        let deadline = started_at + wait_timeout;
        let mut active = self.active.lock().unwrap_or_else(|e| e.into_inner());
        let mut waited = false;
        loop {
            if self.can_admit(&active, class, pending_envelope) {
                active.acquire(class, pending_envelope);
                observability::storage_rpc_admission_class_acquired(class.as_str());
                return UnixStorageNodeRpcAdmissionAcquire::Acquired {
                    permit: UnixStorageNodeRpcAdmissionPermit {
                        admission: Arc::clone(self),
                        class,
                        pending_envelope,
                        observed_active: true,
                    },
                    wait_us: if waited {
                        started_at.elapsed().as_micros()
                    } else {
                        0
                    },
                };
            }
            let Some(remaining) = deadline.checked_duration_since(Instant::now()) else {
                return UnixStorageNodeRpcAdmissionAcquire::TimedOut {
                    wait_us: started_at.elapsed().as_micros(),
                };
            };
            waited = true;
            let (next_active, wait_result) = self
                .capacity_available
                .wait_timeout(active, remaining)
                .unwrap_or_else(|e| e.into_inner());
            active = next_active;
            if wait_result.timed_out() && !self.can_admit(&active, class, pending_envelope) {
                return UnixStorageNodeRpcAdmissionAcquire::TimedOut {
                    wait_us: started_at.elapsed().as_micros(),
                };
            }
        }
    }

    fn can_admit(
        &self,
        active: &UnixStorageNodeRpcAdmissionActive,
        class: UnixStorageNodeRpcAdmissionClass,
        pending_envelope: bool,
    ) -> bool {
        if active.total >= self.limit {
            return false;
        }
        match class {
            UnixStorageNodeRpcAdmissionClass::Control => true,
            UnixStorageNodeRpcAdmissionClass::Completion => {
                active.completion < self.completion_limit
                    && (!pending_envelope || active.pending_envelope < self.pending_envelope_limit)
            }
            UnixStorageNodeRpcAdmissionClass::Progress => {
                active.non_reserved() < self.non_reserved_limit
            }
            UnixStorageNodeRpcAdmissionClass::StartWrite => {
                active.non_reserved() < self.non_reserved_limit
                    && active.start_write < self.current_start_write_limit(active)
            }
            UnixStorageNodeRpcAdmissionClass::Read => {
                active.non_reserved() < self.non_reserved_limit && active.read < self.read_limit
            }
            UnixStorageNodeRpcAdmissionClass::List => {
                active.non_reserved() < self.non_reserved_limit && active.list < self.list_limit
            }
        }
    }

    fn current_start_write_limit(&self, active: &UnixStorageNodeRpcAdmissionActive) -> usize {
        let completion_pressure = active.completion + active.progress;
        self.start_write_limit
            .saturating_sub(completion_pressure)
            .max(self.start_write_floor)
            .min(self.start_write_limit)
    }

    pub(crate) fn wait_timeout_for_class(
        &self,
        class: UnixStorageNodeRpcAdmissionClass,
    ) -> Duration {
        match class {
            UnixStorageNodeRpcAdmissionClass::Control
            | UnixStorageNodeRpcAdmissionClass::Completion
            | UnixStorageNodeRpcAdmissionClass::Progress => self.control_wait_timeout,
            UnixStorageNodeRpcAdmissionClass::StartWrite
            | UnixStorageNodeRpcAdmissionClass::Read
            | UnixStorageNodeRpcAdmissionClass::List => self.wait_timeout,
        }
    }
}

impl Drop for UnixStorageNodeRpcAdmissionPermit {
    fn drop(&mut self) {
        let mut active = self
            .admission
            .active
            .lock()
            .unwrap_or_else(|e| e.into_inner());
        active.release(self.class, self.pending_envelope);
        self.admission.capacity_available.notify_all();
        if self.observed_active {
            observability::storage_rpc_admission_class_released(self.class.as_str());
            self.observed_active = false;
        }
    }
}

pub(crate) fn shared_unix_storage_node_rpc_admission(
    node_id: NodeId,
    socket_path: &Path,
    limit: usize,
) -> Arc<UnixStorageNodeRpcAdmission> {
    shared_unix_storage_node_rpc_admission_with_wait_timeout(
        node_id,
        socket_path,
        limit,
        UNIX_STORAGE_NODE_DEFAULT_RPC_ADMISSION_WAIT_TIMEOUT,
        UNIX_STORAGE_NODE_DEFAULT_RPC_CONTROL_ADMISSION_WAIT_TIMEOUT,
    )
}

pub(crate) fn shared_unix_storage_node_rpc_admission_with_wait_timeout(
    node_id: NodeId,
    socket_path: &Path,
    limit: usize,
    wait_timeout: Duration,
    control_wait_timeout: Duration,
) -> Arc<UnixStorageNodeRpcAdmission> {
    static ADMISSIONS: OnceLock<UnixStorageNodeRpcAdmissionRegistry> = OnceLock::new();

    let key = (node_id.as_u32(), socket_path.to_path_buf());
    let mut admissions = ADMISSIONS
        .get_or_init(|| Mutex::new(BTreeMap::new()))
        .lock()
        .unwrap_or_else(|e| e.into_inner());
    if let Some(admission) = admissions.get(&key).and_then(Weak::upgrade) {
        return admission;
    }
    let admission = Arc::new(UnixStorageNodeRpcAdmission::new_with_wait_timeout(
        limit,
        wait_timeout,
        control_wait_timeout,
    ));
    admissions.insert(key, Arc::downgrade(&admission));
    admission
}
pub(crate) fn storage_rpc_admission_class(
    kind: StorageRpcMessageKind,
) -> UnixStorageNodeRpcAdmissionClass {
    match kind {
        StorageRpcMessageKind::Health => UnixStorageNodeRpcAdmissionClass::Control,

        StorageRpcMessageKind::ShardRead
        | StorageRpcMessageKind::ShardHistoricalRead
        | StorageRpcMessageKind::ShardReadRange
        | StorageRpcMessageKind::ReadHandlesAcquire
        | StorageRpcMessageKind::BucketHeadRaw
        | StorageRpcMessageKind::BucketHeadInfo
        | StorageRpcMessageKind::BucketSnapshotLoad
        | StorageRpcMessageKind::BucketSnapshotPairLoad
        | StorageRpcMessageKind::BucketExecutionGenerations
        | StorageRpcMessageKind::BucketFastPathIdentities
        | StorageRpcMessageKind::BucketSubresourceGet
        | StorageRpcMessageKind::ObjectReadAuthSubjectLoad
        | StorageRpcMessageKind::ObjectReadSnapshotLoad
        | StorageRpcMessageKind::ObjectTagsForSubjectLoad => UnixStorageNodeRpcAdmissionClass::Read,

        StorageRpcMessageKind::ShardScavengerListFiles
        | StorageRpcMessageKind::BucketWriteReservationsList
        | StorageRpcMessageKind::LifecycleSweepBucketsList
        | StorageRpcMessageKind::ObjectLifecycleVersionListLoad
        | StorageRpcMessageKind::ObjectListPage
        | StorageRpcMessageKind::ObjectVersionListPage
        | StorageRpcMessageKind::ObjectMultipartUploadListPage
        | StorageRpcMessageKind::BucketList
        | StorageRpcMessageKind::ObjectMultipartPartsList
        | StorageRpcMessageKind::ObjectMultipartInProgressUploadForListingLoad
        | StorageRpcMessageKind::ObjectStreamUploadsList
        | StorageRpcMessageKind::ObjectStreamUploadsPgList
        | StorageRpcMessageKind::ShardScavengerShardRows
        | StorageRpcMessageKind::ShardScavengerPayloadReferences
        | StorageRpcMessageKind::ShardScavengerObservations
        | StorageRpcMessageKind::PlacedSegmentShardRepairs
        | StorageRpcMessageKind::PlacedSegmentShardBackfills
        | StorageRpcMessageKind::PlacedSegmentShardBackfillCount
        | StorageRpcMessageKind::PlacedSegmentShardBackfillExists => {
            UnixStorageNodeRpcAdmissionClass::List
        }

        StorageRpcMessageKind::BucketCreateCommandBuild
        | StorageRpcMessageKind::ObjectGenerationNext
        | StorageRpcMessageKind::ObjectGenerationReservation
        | StorageRpcMessageKind::ObjectVersionNext
        | StorageRpcMessageKind::BucketWriteReservationAcquire
        | StorageRpcMessageKind::MetadataCommandNextId
        | StorageRpcMessageKind::ObjectMetadataPutCommandBuild
        | StorageRpcMessageKind::ObjectDeleteSpecificCommandBuild
        | StorageRpcMessageKind::ObjectDeleteCurrentCommandBuild
        | StorageRpcMessageKind::ObjectInsertDeleteMarkerCommandBuild
        | StorageRpcMessageKind::ObjectStreamUploadCommandBuild
        | StorageRpcMessageKind::ObjectMultipartUploadCommandBuild => {
            UnixStorageNodeRpcAdmissionClass::StartWrite
        }

        StorageRpcMessageKind::ShardWrite
        | StorageRpcMessageKind::ShardRepairWrite
        | StorageRpcMessageKind::ShardAckRecord
        | StorageRpcMessageKind::ShardAckValidate
        | StorageRpcMessageKind::ShardAckLoad
        | StorageRpcMessageKind::ShardAckHistoricalLoad
        | StorageRpcMessageKind::PlacedSegmentShardRepairRecord
        | StorageRpcMessageKind::PlacedSegmentShardRepairClaimAcquire
        | StorageRpcMessageKind::PlacedSegmentShardRepairClaimComplete
        | StorageRpcMessageKind::PlacedSegmentShardRepairClaimError
        | StorageRpcMessageKind::PlacedSegmentShardRepairResolve
        | StorageRpcMessageKind::PlacedSegmentShardBackfillRecord
        | StorageRpcMessageKind::PlacedSegmentShardBackfillClaimAcquire
        | StorageRpcMessageKind::PlacedSegmentShardBackfillClaimComplete
        | StorageRpcMessageKind::PlacedSegmentShardBackfillClaimError
        | StorageRpcMessageKind::PlacedSegmentShardBackfillResolve
        | StorageRpcMessageKind::BucketWriteReservationValidate
        | StorageRpcMessageKind::BucketWriteReservationHeartbeat
        | StorageRpcMessageKind::ObjectMetadataPutSnapshotLoad
        | StorageRpcMessageKind::ObjectDeleteCurrentSnapshotLoad
        | StorageRpcMessageKind::ObjectDeleteSpecificSnapshotLoad
        | StorageRpcMessageKind::ObjectStreamUploadMatch
        | StorageRpcMessageKind::ObjectMultipartUploadMatch
        | StorageRpcMessageKind::ObjectStreamUploadSessionLoad
        | StorageRpcMessageKind::ObjectStreamUploadSegmentsLoad
        | StorageRpcMessageKind::ObjectStreamSegmentAppendPrepare
        | StorageRpcMessageKind::ObjectMultipartUploadLoad
        | StorageRpcMessageKind::ObjectMultipartInProgressUploadLoad
        | StorageRpcMessageKind::ObjectMultipartManagementLookup
        | StorageRpcMessageKind::BucketMetadataControlPendingMatch
        | StorageRpcMessageKind::LifecycleSweepRoots
        | StorageRpcMessageKind::LifecycleSweepClaimAcquire
        | StorageRpcMessageKind::LifecycleSweepClaimHeartbeat
        | StorageRpcMessageKind::ObjectPayloadReclaimExists
        | StorageRpcMessageKind::ObjectBucketPayloadReclaimRoot
        | StorageRpcMessageKind::ObjectPayloadReclaimRoot
        | StorageRpcMessageKind::ObjectPayloadReclaimLoad
        | StorageRpcMessageKind::ObjectPayloadReclaimClaimAcquire
        | StorageRpcMessageKind::ShardScavengerObservationRecord
        | StorageRpcMessageKind::ShardScavengerObservationResolve => {
            UnixStorageNodeRpcAdmissionClass::Progress
        }

        StorageRpcMessageKind::MetadataCommand
        | StorageRpcMessageKind::MetadataCommandReplicaState
        | StorageRpcMessageKind::MetadataCommandAcceptance
        | StorageRpcMessageKind::MetadataCommandAbandonAcceptance
        | StorageRpcMessageKind::MetadataCommandPendingSlotInsert
        | StorageRpcMessageKind::MetadataCommandPendingSlotRemove
        | StorageRpcMessageKind::MetadataCommandMaxLogIndex
        | StorageRpcMessageKind::MetadataCommandPendingEnvelope
        | StorageRpcMessageKind::MetadataCommandValidateReplayState
        | StorageRpcMessageKind::MetadataCommandValidateReplayStatePreservingPending
        | StorageRpcMessageKind::MetadataCommandReplicaStateCanInitialize
        | StorageRpcMessageKind::MetadataCommandTransferStateAdopt
        | StorageRpcMessageKind::MetadataCommandTransferEmptyStateInitialize
        | StorageRpcMessageKind::MetadataCommandTransferMatchingStateInitialize
        | StorageRpcMessageKind::MetadataCommandTransferCheckpointBaseInstall
        | StorageRpcMessageKind::MetadataCommandCheckpointExport
        | StorageRpcMessageKind::MetadataCommandCheckpointCandidates
        | StorageRpcMessageKind::MetadataCommandCheckpointRecordCurrent
        | StorageRpcMessageKind::MetadataCommandAppliedLogHashes
        | StorageRpcMessageKind::MetadataCommandRetainedLogHashes
        | StorageRpcMessageKind::MetadataCommandRetainedLogEntries
        | StorageRpcMessageKind::MetadataCommandMatchingAppliedLog
        | StorageRpcMessageKind::MetadataCommandAbandoned
        | StorageRpcMessageKind::MetadataCommandRecordAbandoned
        | StorageRpcMessageKind::MetadataCommandPendingSlotReplace
        | StorageRpcMessageKind::MetadataCommandBucketControlPendingSlotInsert
        | StorageRpcMessageKind::MetadataCommandApplyAndRecord
        | StorageRpcMessageKind::MetadataCommandPeeringReplayApplyAndRecord
        | StorageRpcMessageKind::MetadataCommandPgLockAcquire
        | StorageRpcMessageKind::MetadataCommandPgLockRelease
        | StorageRpcMessageKind::ReadHandlesRelease
        | StorageRpcMessageKind::ClaimHeartbeat
        | StorageRpcMessageKind::ClaimRelease
        | StorageRpcMessageKind::ProofRelease
        | StorageRpcMessageKind::ShardDelete
        | StorageRpcMessageKind::ShardAckDelete
        | StorageRpcMessageKind::BucketWriteReservationRelease
        | StorageRpcMessageKind::DirectPutCommitSnapshotLoad
        | StorageRpcMessageKind::DirectPutCommitCommandBuild
        | StorageRpcMessageKind::CompletedMultipartOrderCommandBuild
        | StorageRpcMessageKind::ObjectStreamPutFinalizeSnapshotLoad
        | StorageRpcMessageKind::ObjectStreamPutCommitCommandBuild
        | StorageRpcMessageKind::ObjectStreamPartFinalizeSnapshotLoad
        | StorageRpcMessageKind::ObjectStreamPartCommitCommandBuild
        | StorageRpcMessageKind::ObjectMultipartCompleteCommandBuild
        | StorageRpcMessageKind::ObjectMultipartAbortCommandBuild
        | StorageRpcMessageKind::ObjectMultipartAuthorizedAbortCommandBuild
        | StorageRpcMessageKind::ObjectMultipartCompletionStaleSourceLoad
        | StorageRpcMessageKind::ObjectMultipartAbortCleanupLoad
        | StorageRpcMessageKind::ObjectMultipartCompletionSnapshotLoad
        | StorageRpcMessageKind::ObjectMultipartCompletionPreflightLoad
        | StorageRpcMessageKind::ObjectCompletedMultipartUploadsList
        | StorageRpcMessageKind::BucketWriteDrainBegin
        | StorageRpcMessageKind::BucketWriteDrainClear
        | StorageRpcMessageKind::BucketWriteDrainClearExpired
        | StorageRpcMessageKind::BucketWriteDrainHeartbeat
        | StorageRpcMessageKind::BucketDeleteFinalized
        | StorageRpcMessageKind::BucketDeleteFinalizeRoots
        | StorageRpcMessageKind::BucketDeleteFinalizeClaimAcquire
        | StorageRpcMessageKind::BucketDeleteFinalizeClaimRelease
        | StorageRpcMessageKind::BucketMarkDeletingCommandBuild
        | StorageRpcMessageKind::BucketWriteDrainExists
        | StorageRpcMessageKind::ObjectPayloadReclaimClaimRelease
        | StorageRpcMessageKind::LifecycleSweepClaimError
        | StorageRpcMessageKind::LifecycleSweepClaimRelease => {
            UnixStorageNodeRpcAdmissionClass::Completion
        }

        StorageRpcMessageKind::BucketMetadataControlCommandBuild => {
            UnixStorageNodeRpcAdmissionClass::StartWrite
        }
    }
}

pub(crate) fn listing_probe_admission_class(limit: u32) -> UnixStorageNodeRpcAdmissionClass {
    if limit <= 1 {
        UnixStorageNodeRpcAdmissionClass::Completion
    } else {
        UnixStorageNodeRpcAdmissionClass::List
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;
    use std::thread;

    #[test]
    fn unix_storage_node_rpc_admission_waits_for_released_capacity() {
        let admission = Arc::new(UnixStorageNodeRpcAdmission::new_with_wait_timeout(
            1,
            Duration::from_secs(1),
            Duration::from_secs(1),
        ));
        let held = admission.try_acquire_for_test().unwrap();
        let (attempt_tx, attempt_rx) = std::sync::mpsc::channel();
        let (acquired_tx, acquired_rx) = std::sync::mpsc::channel();
        let admission_for_thread = Arc::clone(&admission);

        let join = thread::spawn(move || {
            attempt_tx.send(()).unwrap();
            match admission_for_thread.acquire(UnixStorageNodeRpcAdmissionClass::Control) {
                UnixStorageNodeRpcAdmissionAcquire::Acquired { permit, wait_us } => {
                    acquired_tx.send(wait_us > 0).unwrap();
                    Some(permit)
                }
                UnixStorageNodeRpcAdmissionAcquire::TimedOut { .. } => {
                    acquired_tx.send(false).unwrap();
                    None
                }
            }
        });

        attempt_rx.recv_timeout(Duration::from_secs(1)).unwrap();
        assert!(acquired_rx.recv_timeout(Duration::from_millis(50)).is_err());
        drop(held);
        assert!(acquired_rx.recv_timeout(Duration::from_secs(1)).unwrap());
        let acquired = join.join().unwrap();
        assert!(acquired.is_some());
    }

    #[test]
    fn unix_storage_node_rpc_admission_times_out_under_sustained_overload() {
        let admission = Arc::new(UnixStorageNodeRpcAdmission::new_with_wait_timeout(
            1,
            Duration::from_millis(10),
            Duration::from_millis(10),
        ));
        let _held = admission.try_acquire_for_test().unwrap();

        assert!(matches!(
            admission.acquire(UnixStorageNodeRpcAdmissionClass::Control),
            UnixStorageNodeRpcAdmissionAcquire::TimedOut { .. }
        ));
    }

    #[test]
    fn unix_storage_node_rpc_admission_splits_read_from_list_work() {
        let admission = Arc::new(UnixStorageNodeRpcAdmission::new_with_wait_timeout(
            4,
            Duration::from_millis(10),
            Duration::from_millis(10),
        ));
        let held_list = match admission.acquire(UnixStorageNodeRpcAdmissionClass::List) {
            UnixStorageNodeRpcAdmissionAcquire::Acquired { permit, .. } => permit,
            UnixStorageNodeRpcAdmissionAcquire::TimedOut { .. } => {
                panic!("first list admission unexpectedly timed out")
            }
        };

        assert!(matches!(
            admission.acquire(UnixStorageNodeRpcAdmissionClass::List),
            UnixStorageNodeRpcAdmissionAcquire::TimedOut { .. }
        ));

        let read = match admission.acquire(UnixStorageNodeRpcAdmissionClass::Read) {
            UnixStorageNodeRpcAdmissionAcquire::Acquired { permit, .. } => permit,
            UnixStorageNodeRpcAdmissionAcquire::TimedOut { .. } => {
                panic!("read admission should not wait behind list cap")
            }
        };

        drop(read);
        drop(held_list);
    }

    #[test]
    fn unix_storage_node_rpc_admission_minimum_supports_nested_read_and_mixed_work() {
        let admission = Arc::new(UnixStorageNodeRpcAdmission::new_with_wait_timeout(
            UNIX_STORAGE_NODE_MIN_RPC_ADMISSION_LIMIT,
            Duration::from_millis(10),
            Duration::from_millis(10),
        ));
        let held_read_handle = match admission.acquire(UnixStorageNodeRpcAdmissionClass::Read) {
            UnixStorageNodeRpcAdmissionAcquire::Acquired { permit, .. } => permit,
            UnixStorageNodeRpcAdmissionAcquire::TimedOut { .. } => {
                panic!("minimum admission limit should allow a held read-handle session")
            }
        };
        let shard_read = match admission.acquire(UnixStorageNodeRpcAdmissionClass::Read) {
            UnixStorageNodeRpcAdmissionAcquire::Acquired { permit, .. } => permit,
            UnixStorageNodeRpcAdmissionAcquire::TimedOut { .. } => {
                panic!("minimum admission limit should allow shard read while handle is held")
            }
        };
        let list = match admission.acquire(UnixStorageNodeRpcAdmissionClass::List) {
            UnixStorageNodeRpcAdmissionAcquire::Acquired { permit, .. } => permit,
            UnixStorageNodeRpcAdmissionAcquire::TimedOut { .. } => {
                panic!("minimum admission limit should leave room for list work")
            }
        };
        let progress = match admission.acquire(UnixStorageNodeRpcAdmissionClass::Progress) {
            UnixStorageNodeRpcAdmissionAcquire::Acquired { permit, .. } => permit,
            UnixStorageNodeRpcAdmissionAcquire::TimedOut { .. } => {
                panic!("minimum admission limit should leave room for progress work")
            }
        };
        let start_write = match admission.acquire(UnixStorageNodeRpcAdmissionClass::StartWrite) {
            UnixStorageNodeRpcAdmissionAcquire::Acquired { permit, .. } => permit,
            UnixStorageNodeRpcAdmissionAcquire::TimedOut { .. } => {
                panic!("minimum admission limit should leave a start-write floor")
            }
        };
        let completion = match admission.acquire(UnixStorageNodeRpcAdmissionClass::Completion) {
            UnixStorageNodeRpcAdmissionAcquire::Acquired { permit, .. } => permit,
            UnixStorageNodeRpcAdmissionAcquire::TimedOut { .. } => {
                panic!("minimum admission limit should preserve completion capacity")
            }
        };
        let control = match admission.acquire(UnixStorageNodeRpcAdmissionClass::Control) {
            UnixStorageNodeRpcAdmissionAcquire::Acquired { permit, .. } => permit,
            UnixStorageNodeRpcAdmissionAcquire::TimedOut { .. } => {
                panic!("minimum admission limit should preserve control capacity")
            }
        };

        drop(control);
        drop(completion);
        drop(start_write);
        drop(progress);
        drop(list);
        drop(shard_read);
        drop(held_read_handle);
    }

    #[test]
    fn unix_storage_node_rpc_admission_biases_completion_over_new_starts() {
        let admission = Arc::new(UnixStorageNodeRpcAdmission::new_with_wait_timeout(
            4,
            Duration::from_millis(10),
            Duration::from_millis(10),
        ));
        let held_progress_a = match admission.acquire(UnixStorageNodeRpcAdmissionClass::Progress) {
            UnixStorageNodeRpcAdmissionAcquire::Acquired { permit, .. } => permit,
            UnixStorageNodeRpcAdmissionAcquire::TimedOut { .. } => {
                panic!("first progress admission unexpectedly timed out")
            }
        };
        let held_progress_b = match admission.acquire(UnixStorageNodeRpcAdmissionClass::Progress) {
            UnixStorageNodeRpcAdmissionAcquire::Acquired { permit, .. } => permit,
            UnixStorageNodeRpcAdmissionAcquire::TimedOut { .. } => {
                panic!("second progress admission unexpectedly timed out")
            }
        };
        let held_start = match admission.acquire(UnixStorageNodeRpcAdmissionClass::StartWrite) {
            UnixStorageNodeRpcAdmissionAcquire::Acquired { permit, .. } => permit,
            UnixStorageNodeRpcAdmissionAcquire::TimedOut { .. } => {
                panic!("start-write floor admission unexpectedly timed out")
            }
        };

        assert!(matches!(
            admission.acquire(UnixStorageNodeRpcAdmissionClass::StartWrite),
            UnixStorageNodeRpcAdmissionAcquire::TimedOut { .. }
        ));

        let completion = match admission.acquire(UnixStorageNodeRpcAdmissionClass::Completion) {
            UnixStorageNodeRpcAdmissionAcquire::Acquired { permit, .. } => permit,
            UnixStorageNodeRpcAdmissionAcquire::TimedOut { .. } => {
                panic!("completion admission should use remaining capacity")
            }
        };

        drop(completion);
        drop(held_start);
        drop(held_progress_b);
        drop(held_progress_a);
    }

    #[test]
    fn unix_storage_node_rpc_admission_completion_progresses_when_start_write_is_saturated() {
        let admission = Arc::new(UnixStorageNodeRpcAdmission::new_with_wait_timeout(
            4,
            Duration::from_millis(10),
            Duration::from_millis(10),
        ));
        let held_progress_a = match admission.acquire(UnixStorageNodeRpcAdmissionClass::Progress) {
            UnixStorageNodeRpcAdmissionAcquire::Acquired { permit, .. } => permit,
            UnixStorageNodeRpcAdmissionAcquire::TimedOut { .. } => {
                panic!("first progress admission unexpectedly timed out")
            }
        };
        let held_progress_b = match admission.acquire(UnixStorageNodeRpcAdmissionClass::Progress) {
            UnixStorageNodeRpcAdmissionAcquire::Acquired { permit, .. } => permit,
            UnixStorageNodeRpcAdmissionAcquire::TimedOut { .. } => {
                panic!("second progress admission unexpectedly timed out")
            }
        };
        let held_start = match admission.acquire(UnixStorageNodeRpcAdmissionClass::StartWrite) {
            UnixStorageNodeRpcAdmissionAcquire::Acquired { permit, .. } => permit,
            UnixStorageNodeRpcAdmissionAcquire::TimedOut { .. } => {
                panic!("start-write floor admission unexpectedly timed out")
            }
        };

        assert!(matches!(
            admission.acquire(UnixStorageNodeRpcAdmissionClass::StartWrite),
            UnixStorageNodeRpcAdmissionAcquire::TimedOut { .. }
        ));

        let completion = match admission.acquire(UnixStorageNodeRpcAdmissionClass::Completion) {
            UnixStorageNodeRpcAdmissionAcquire::Acquired { permit, .. } => permit,
            UnixStorageNodeRpcAdmissionAcquire::TimedOut { .. } => {
                panic!("completion helper admission should not wait behind start-write saturation")
            }
        };

        drop(completion);
        drop(held_start);
        drop(held_progress_b);
        drop(held_progress_a);
    }

    #[test]
    fn unix_storage_node_rpc_admission_completion_cannot_exhaust_start_write_floor() {
        let admission = Arc::new(UnixStorageNodeRpcAdmission::new_with_wait_timeout(
            32,
            Duration::from_millis(10),
            Duration::from_millis(10),
        ));
        let mut completions = Vec::new();
        for _ in 0..28 {
            let completion = match admission.acquire(UnixStorageNodeRpcAdmissionClass::Completion) {
                UnixStorageNodeRpcAdmissionAcquire::Acquired { permit, .. } => permit,
                UnixStorageNodeRpcAdmissionAcquire::TimedOut { .. } => {
                    panic!("completion admission unexpectedly timed out before its cap")
                }
            };
            completions.push(completion);
        }

        assert!(matches!(
            admission.acquire(UnixStorageNodeRpcAdmissionClass::Completion),
            UnixStorageNodeRpcAdmissionAcquire::TimedOut { .. }
        ));

        let start_write = match admission.acquire(UnixStorageNodeRpcAdmissionClass::StartWrite) {
            UnixStorageNodeRpcAdmissionAcquire::Acquired { permit, .. } => permit,
            UnixStorageNodeRpcAdmissionAcquire::TimedOut { .. } => {
                panic!("completion saturation should preserve the start-write floor")
            }
        };

        drop(start_write);
        drop(completions);
    }

    #[test]
    fn unix_storage_node_rpc_admission_pending_envelopes_cannot_exhaust_completion() {
        let admission = Arc::new(UnixStorageNodeRpcAdmission::new_with_wait_timeout(
            32,
            Duration::from_millis(10),
            Duration::from_millis(10),
        ));
        let mut pending_envelopes = Vec::new();
        for _ in 0..14 {
            let pending_envelope = match admission.acquire_with_kind(
                UnixStorageNodeRpcAdmissionClass::Completion,
                StorageRpcMessageKind::MetadataCommandPendingEnvelope,
            ) {
                UnixStorageNodeRpcAdmissionAcquire::Acquired { permit, .. } => permit,
                UnixStorageNodeRpcAdmissionAcquire::TimedOut { .. } => {
                    panic!("pending-envelope admission unexpectedly timed out before its cap")
                }
            };
            pending_envelopes.push(pending_envelope);
        }

        assert!(matches!(
            admission.acquire_with_kind(
                UnixStorageNodeRpcAdmissionClass::Completion,
                StorageRpcMessageKind::MetadataCommandPendingEnvelope,
            ),
            UnixStorageNodeRpcAdmissionAcquire::TimedOut { .. }
        ));

        let release = match admission.acquire_with_kind(
            UnixStorageNodeRpcAdmissionClass::Completion,
            StorageRpcMessageKind::BucketWriteReservationRelease,
        ) {
            UnixStorageNodeRpcAdmissionAcquire::Acquired { permit, .. } => permit,
            UnixStorageNodeRpcAdmissionAcquire::TimedOut { .. } => {
                panic!("pending-envelope saturation should preserve completion capacity")
            }
        };

        drop(release);
        drop(pending_envelopes);
    }

    #[test]
    fn unix_storage_node_rpc_admission_progress_cannot_consume_completion_reserve() {
        let admission = Arc::new(UnixStorageNodeRpcAdmission::new_with_wait_timeout(
            4,
            Duration::from_millis(10),
            Duration::from_millis(10),
        ));
        let held_progress_a = match admission.acquire(UnixStorageNodeRpcAdmissionClass::Progress) {
            UnixStorageNodeRpcAdmissionAcquire::Acquired { permit, .. } => permit,
            UnixStorageNodeRpcAdmissionAcquire::TimedOut { .. } => {
                panic!("first progress admission unexpectedly timed out")
            }
        };
        let held_progress_b = match admission.acquire(UnixStorageNodeRpcAdmissionClass::Progress) {
            UnixStorageNodeRpcAdmissionAcquire::Acquired { permit, .. } => permit,
            UnixStorageNodeRpcAdmissionAcquire::TimedOut { .. } => {
                panic!("second progress admission unexpectedly timed out")
            }
        };
        let held_progress_c = match admission.acquire(UnixStorageNodeRpcAdmissionClass::Progress) {
            UnixStorageNodeRpcAdmissionAcquire::Acquired { permit, .. } => permit,
            UnixStorageNodeRpcAdmissionAcquire::TimedOut { .. } => {
                panic!("third progress admission unexpectedly timed out")
            }
        };

        assert!(matches!(
            admission.acquire(UnixStorageNodeRpcAdmissionClass::Progress),
            UnixStorageNodeRpcAdmissionAcquire::TimedOut { .. }
        ));

        let completion = match admission.acquire(UnixStorageNodeRpcAdmissionClass::Completion) {
            UnixStorageNodeRpcAdmissionAcquire::Acquired { permit, .. } => permit,
            UnixStorageNodeRpcAdmissionAcquire::TimedOut { .. } => {
                panic!("completion admission should use reserved capacity")
            }
        };

        drop(completion);
        drop(held_progress_c);
        drop(held_progress_b);
        drop(held_progress_a);
    }

    #[test]
    fn shard_write_uses_progress_admission_class() {
        assert_eq!(
            storage_rpc_admission_class(StorageRpcMessageKind::ShardWrite),
            UnixStorageNodeRpcAdmissionClass::Progress
        );
    }

    #[test]
    fn listing_probe_admission_class_reserves_single_row_emptiness_probes() {
        assert_eq!(
            listing_probe_admission_class(1),
            UnixStorageNodeRpcAdmissionClass::Completion
        );
        assert_eq!(
            listing_probe_admission_class(2),
            UnixStorageNodeRpcAdmissionClass::List
        );
    }

    #[test]
    fn completed_multipart_cleanup_listing_uses_completion_admission_class() {
        assert_eq!(
            storage_rpc_admission_class(StorageRpcMessageKind::ObjectCompletedMultipartUploadsList),
            UnixStorageNodeRpcAdmissionClass::Completion
        );
    }

    #[test]
    fn metadata_command_critical_section_uses_completion_admission_class() {
        assert_eq!(
            storage_rpc_admission_class(StorageRpcMessageKind::MetadataCommandPgLockAcquire),
            UnixStorageNodeRpcAdmissionClass::Completion
        );
    }

    #[test]
    fn direct_put_commit_rpcs_use_completion_admission_class() {
        assert_eq!(
            storage_rpc_admission_class(StorageRpcMessageKind::DirectPutCommitSnapshotLoad),
            UnixStorageNodeRpcAdmissionClass::Completion
        );
        assert_eq!(
            storage_rpc_admission_class(StorageRpcMessageKind::DirectPutCommitCommandBuild),
            UnixStorageNodeRpcAdmissionClass::Completion
        );
    }

    #[test]
    fn ordinary_object_mutation_command_builds_do_not_use_control_reserve() {
        for kind in [
            StorageRpcMessageKind::ObjectMetadataPutCommandBuild,
            StorageRpcMessageKind::ObjectDeleteCurrentCommandBuild,
            StorageRpcMessageKind::ObjectDeleteSpecificCommandBuild,
            StorageRpcMessageKind::ObjectInsertDeleteMarkerCommandBuild,
        ] {
            assert_eq!(
                storage_rpc_admission_class(kind),
                UnixStorageNodeRpcAdmissionClass::StartWrite
            );
        }
    }

    #[test]
    fn context_sensitive_helper_rpcs_default_to_start_write_admission_class() {
        for kind in [
            StorageRpcMessageKind::ObjectVersionNext,
            StorageRpcMessageKind::BucketWriteReservationAcquire,
            StorageRpcMessageKind::MetadataCommandNextId,
        ] {
            assert_eq!(
                storage_rpc_admission_class(kind),
                UnixStorageNodeRpcAdmissionClass::StartWrite
            );
        }
    }

    #[test]
    fn read_support_rpcs_use_read_admission_class() {
        assert_eq!(
            storage_rpc_admission_class(StorageRpcMessageKind::ObjectReadAuthSubjectLoad),
            UnixStorageNodeRpcAdmissionClass::Read
        );
        assert_eq!(
            storage_rpc_admission_class(StorageRpcMessageKind::ObjectReadSnapshotLoad),
            UnixStorageNodeRpcAdmissionClass::Read
        );
        assert_eq!(
            storage_rpc_admission_class(StorageRpcMessageKind::ObjectTagsForSubjectLoad),
            UnixStorageNodeRpcAdmissionClass::Read
        );
    }

    #[test]
    fn multipart_listing_support_uses_list_admission_class() {
        assert_eq!(
            storage_rpc_admission_class(
                StorageRpcMessageKind::ObjectMultipartInProgressUploadForListingLoad
            ),
            UnixStorageNodeRpcAdmissionClass::List
        );
    }
}
