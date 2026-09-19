// Copyright The Argmin Authors.
// SPDX-License-Identifier: Apache-2.0

use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::panic::{self, AssertUnwindSafe};
use std::sync::mpsc::{self, Receiver, SyncSender, TryRecvError, TrySendError};
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::{Duration, Instant};

use crate::control_plane::{
    ClusterControlSnapshot, ControlPlaneError, FileControlPlaneStore,
    MetadataTransferStagingMaintenanceCursor, SingleAuthorityControlPlane,
    UnavailablePgReconciliationCompletionBatch, UnavailablePgReconciliationCursor,
    UnavailablePgReconciliationPollBatch, UnavailablePgReconciliationStage,
    UnavailablePgReconciliationWork,
};
use crate::control_plane_command::{
    FinalizeMetadataTransferStagingGenerationRequest,
    UnavailablePgStagingIntentAuthorizationRequest, UnavailablePgTransitionInstallRequest,
};
use crate::live_pg_transfer::{
    AuthorizedUnavailablePgMetadataTransfer, CleanupUnavailablePgMetadataTransfer,
    PreparedUnavailablePgMetadataTransfer, StagedUnavailablePgMetadataTransfer,
    TombstonedUnavailablePgMetadataTransfer,
};
use crate::{ClusterEpoch, ControlPlaneRaftAuthorityHost, LivePgMetadataTransferAdmin, PgId};

const RETRY_BACKOFF: Duration = Duration::from_secs(1);
const TRANSFER_WORKER_COUNT: usize = 4;
const STAGING_AUTHORIZATION_OBSERVATION_WAIT: Duration = Duration::from_secs(2);
const STAGING_AUTHORIZATION_OBSERVATION_RETRY: Duration = Duration::from_millis(250);
const STAGING_EVIDENCE_INSTALL_WAIT: Duration = Duration::from_secs(5);

struct TransferCompletion {
    work: UnavailablePgReconciliationWork,
    result: Result<TransferOutcome, ReconciliationTransferError>,
}

enum TransferJob {
    Legacy(UnavailablePgReconciliationWork),
    Prepare(UnavailablePgReconciliationWork),
    ResumeAuthorized(UnavailablePgReconciliationWork, Box<ClusterControlSnapshot>),
    Stage(AuthorizedUnavailablePgMetadataTransfer),
    ResumeInstalled(UnavailablePgReconciliationWork, Box<ClusterControlSnapshot>),
    ResumeCleanup(UnavailablePgReconciliationWork, Box<ClusterControlSnapshot>),
    Rebase(StagedUnavailablePgMetadataTransfer, ClusterEpoch),
    Import(StagedUnavailablePgMetadataTransfer),
    Tombstone(
        CleanupUnavailablePgMetadataTransfer,
        Box<ClusterControlSnapshot>,
    ),
}

impl TransferJob {
    fn work(&self) -> &UnavailablePgReconciliationWork {
        match self {
            Self::Legacy(work) | Self::Prepare(work) => work,
            Self::ResumeAuthorized(work, _)
            | Self::ResumeInstalled(work, _)
            | Self::ResumeCleanup(work, _) => work,
            Self::Stage(authorized) => authorized.work(),
            Self::Rebase(staged, _) | Self::Import(staged) => staged.work(),
            Self::Tombstone(cleanup, _) => cleanup.work(),
        }
    }

    fn metric_stage(&self) -> observability::UnavailablePgWorkerStage {
        match self {
            Self::Legacy(_) | Self::Prepare(_) => observability::UnavailablePgWorkerStage::Prepare,
            Self::ResumeAuthorized(_, _) | Self::Stage(_) | Self::Rebase(_, _) => {
                observability::UnavailablePgWorkerStage::Stage
            }
            Self::ResumeInstalled(_, _) | Self::Import(_) => {
                observability::UnavailablePgWorkerStage::Import
            }
            Self::ResumeCleanup(_, _) | Self::Tombstone(_, _) => {
                observability::UnavailablePgWorkerStage::Tombstone
            }
        }
    }
}

enum TransferOutcome {
    LegacyReady,
    Prepared(PreparedUnavailablePgMetadataTransfer),
    Authorized(AuthorizedUnavailablePgMetadataTransfer),
    ReadyInstall(StagedUnavailablePgMetadataTransfer),
    ReadyImport(StagedUnavailablePgMetadataTransfer),
    ReadyCleanup(
        CleanupUnavailablePgMetadataTransfer,
        Box<ClusterControlSnapshot>,
    ),
    Imported(StagedUnavailablePgMetadataTransfer),
    Tombstoned(TombstonedUnavailablePgMetadataTransfer),
}

type LegacyTransfer = dyn Fn(&UnavailablePgReconciliationWork) -> Result<(), ReconciliationTransferError>
    + Send
    + Sync;

enum TransferExecutor {
    #[allow(dead_code)] // Used only by owner-local unit-test executors.
    Legacy(Arc<LegacyTransfer>),
    Staged(Arc<LivePgMetadataTransferAdmin>),
}

impl TransferExecutor {
    fn execute(&self, job: TransferJob) -> Result<TransferOutcome, ReconciliationTransferError> {
        match (self, job) {
            (Self::Legacy(transfer), TransferJob::Legacy(work)) => {
                transfer(&work)?;
                Ok(TransferOutcome::LegacyReady)
            }
            (Self::Staged(admin), TransferJob::Prepare(work)) => admin
                .prepare_unavailable_pg_reconciliation_staging(work)
                .map(TransferOutcome::Prepared)
                .map_err(reconciliation_transfer_failure),
            (Self::Staged(admin), TransferJob::ResumeAuthorized(work, snapshot)) => admin
                .resume_authorized_unavailable_pg_reconciliation(work, &snapshot)
                .map(TransferOutcome::Authorized)
                .map_err(reconciliation_transfer_failure),
            (Self::Staged(admin), TransferJob::Stage(authorized)) => {
                let observation_deadline = Instant::now() + STAGING_AUTHORIZATION_OBSERVATION_WAIT;
                let published = loop {
                    match admin.stage_prepared_unavailable_pg_reconciliation(&authorized) {
                        Ok(published) => break published,
                        Err(error) if error.is_staging_authorization_not_observed() => {
                            let remaining =
                                observation_deadline.saturating_duration_since(Instant::now());
                            if remaining.is_zero() {
                                return Err(reconciliation_transfer_error(error));
                            }
                            // The committed authorization is epoch-neutral, but the
                            // destination may not have refreshed its runtime map yet.
                            thread::sleep(remaining.min(STAGING_AUTHORIZATION_OBSERVATION_RETRY));
                        }
                        Err(error) => return Err(reconciliation_transfer_error(error)),
                    }
                };
                authorized
                    .bind_publications(published)
                    .map(TransferOutcome::ReadyInstall)
                    .map_err(reconciliation_transfer_error)
            }
            (Self::Staged(admin), TransferJob::ResumeInstalled(work, snapshot)) => admin
                .resume_installed_unavailable_pg_reconciliation(work, &snapshot)
                .map(TransferOutcome::ReadyImport)
                .map_err(reconciliation_transfer_failure),
            (Self::Staged(admin), TransferJob::ResumeCleanup(work, snapshot)) => admin
                .resume_cleanup_unavailable_pg_reconciliation(work, &snapshot)
                .map(|cleanup| TransferOutcome::ReadyCleanup(cleanup, snapshot))
                .map_err(reconciliation_transfer_failure),
            (Self::Staged(admin), TransferJob::Rebase(mut staged, target_epoch)) => {
                admin
                    .rebase_staged_unavailable_pg_reconciliation(&mut staged, target_epoch)
                    .map_err(reconciliation_transfer_failure)?;
                Ok(TransferOutcome::ReadyInstall(staged))
            }
            (Self::Staged(admin), TransferJob::Import(staged)) => {
                admin
                    .import_staged_unavailable_pg_reconciliation(&staged)
                    .map_err(reconciliation_transfer_failure)?;
                Ok(TransferOutcome::Imported(staged))
            }
            (Self::Staged(admin), TransferJob::Tombstone(staged, snapshot)) => admin
                .tombstone_unavailable_pg_reconciliation(&staged, &snapshot)
                .map(TransferOutcome::Tombstoned)
                .map_err(reconciliation_transfer_failure),
            (Self::Legacy(_), _) | (Self::Staged(_), TransferJob::Legacy(_)) => {
                panic!("unavailable PG reconciliation executor received the wrong job kind")
            }
        }
    }
}

fn reconciliation_transfer_failure(
    error: crate::LivePgMetadataTransferError,
) -> ReconciliationTransferError {
    reconciliation_transfer_error(error)
}

fn reconciliation_transfer_error(
    error: crate::LivePgMetadataTransferError,
) -> ReconciliationTransferError {
    let diagnostic = error.to_string();
    if error.is_fatal() {
        ReconciliationTransferError::Fatal(diagnostic)
    } else {
        ReconciliationTransferError::Retryable(diagnostic)
    }
}

fn record_authority_stage_result<T>(
    stage: observability::UnavailablePgWorkerStage,
    started_at: Instant,
    result: &Result<T, ControlPlaneError>,
) {
    let outcome = match result {
        Ok(_) => observability::UnavailablePgWorkerStageOutcome::Succeeded,
        Err(error) if reconciliation_completion_error_is_fatal(error) => {
            observability::UnavailablePgWorkerStageOutcome::Fatal
        }
        Err(_) => observability::UnavailablePgWorkerStageOutcome::Deferred,
    };
    record_worker_stage_outcome(stage, started_at, outcome);
}

fn record_worker_stage_outcome(
    stage: observability::UnavailablePgWorkerStage,
    started_at: Instant,
    outcome: observability::UnavailablePgWorkerStageOutcome,
) {
    observability::record_unavailable_pg_worker_stage(stage, outcome, started_at.elapsed());
}

enum TransferWorkerEvent {
    Completed(Box<TransferCompletion>),
    Panicked,
}

enum ReconciliationTransferError {
    Retryable(String),
    Fatal(String),
}

enum ActivationOwner {
    Legacy(UnavailablePgReconciliationWork),
    Staged(Box<StagedUnavailablePgMetadataTransfer>),
}

impl ActivationOwner {
    fn work(&self) -> &UnavailablePgReconciliationWork {
        match self {
            Self::Legacy(work) => work,
            Self::Staged(staged) => staged.work(),
        }
    }
}

impl ReconciliationTransferError {
    fn diagnostic(self) -> String {
        match self {
            Self::Retryable(diagnostic) | Self::Fatal(diagnostic) => diagnostic,
        }
    }

    fn is_fatal(&self) -> bool {
        matches!(self, Self::Fatal(_))
    }
}

trait ReconciliationAuthority {
    fn reconciliation_snapshot(&self) -> Result<ClusterControlSnapshot, ControlPlaneError>;

    fn poll_reconciliation_batch(
        &mut self,
        cursor: &mut UnavailablePgReconciliationCursor,
        now_ms: u64,
    ) -> Result<UnavailablePgReconciliationPollBatch, ControlPlaneError>;

    fn complete_reconciliation_batch(
        &mut self,
        work: &[UnavailablePgReconciliationWork],
        now_ms: u64,
    ) -> Result<UnavailablePgReconciliationCompletionBatch, ControlPlaneError>;

    fn authorize_staging_batch(
        &mut self,
        requests: &[UnavailablePgStagingIntentAuthorizationRequest],
    ) -> Result<ClusterControlSnapshot, ControlPlaneError>;

    fn install_staged_batch(
        &mut self,
        requests: &[UnavailablePgTransitionInstallRequest],
        expected_destination_epoch: ClusterEpoch,
    ) -> Result<ClusterControlSnapshot, ControlPlaneError>;

    fn finalize_staging_generation(
        &mut self,
        cleanup: FinalizeMetadataTransferStagingGenerationRequest,
    ) -> Result<ClusterControlSnapshot, ControlPlaneError>;

    fn maintain_staging_evidence(
        &mut self,
        cursor: &mut MetadataTransferStagingMaintenanceCursor,
    ) -> Result<bool, ControlPlaneError>;
}

impl ReconciliationAuthority for ControlPlaneRaftAuthorityHost {
    fn reconciliation_snapshot(&self) -> Result<ClusterControlSnapshot, ControlPlaneError> {
        self.unavailable_pg_reconciliation_snapshot()
    }

    fn poll_reconciliation_batch(
        &mut self,
        cursor: &mut UnavailablePgReconciliationCursor,
        now_ms: u64,
    ) -> Result<UnavailablePgReconciliationPollBatch, ControlPlaneError> {
        self.poll_unavailable_pg_reconciliation_batch(cursor, now_ms)
    }

    fn complete_reconciliation_batch(
        &mut self,
        work: &[UnavailablePgReconciliationWork],
        now_ms: u64,
    ) -> Result<UnavailablePgReconciliationCompletionBatch, ControlPlaneError> {
        self.complete_unavailable_pg_reconciliation_batch(work, now_ms)
    }

    fn authorize_staging_batch(
        &mut self,
        requests: &[UnavailablePgStagingIntentAuthorizationRequest],
    ) -> Result<ClusterControlSnapshot, ControlPlaneError> {
        self.authorize_unavailable_pg_staging_intents_batch(requests)
    }

    fn install_staged_batch(
        &mut self,
        requests: &[UnavailablePgTransitionInstallRequest],
        expected_destination_epoch: ClusterEpoch,
    ) -> Result<ClusterControlSnapshot, ControlPlaneError> {
        self.install_unavailable_pg_placement_transitions_batch(
            requests,
            expected_destination_epoch,
        )
    }

    fn finalize_staging_generation(
        &mut self,
        cleanup: FinalizeMetadataTransferStagingGenerationRequest,
    ) -> Result<ClusterControlSnapshot, ControlPlaneError> {
        self.finalize_metadata_transfer_staging_generation(cleanup)
    }

    fn maintain_staging_evidence(
        &mut self,
        cursor: &mut MetadataTransferStagingMaintenanceCursor,
    ) -> Result<bool, ControlPlaneError> {
        self.maintain_metadata_transfer_staging_evidence_once(cursor)
    }
}

impl ReconciliationAuthority for SingleAuthorityControlPlane<FileControlPlaneStore> {
    fn reconciliation_snapshot(&self) -> Result<ClusterControlSnapshot, ControlPlaneError> {
        Ok(self.snapshot().clone())
    }

    fn poll_reconciliation_batch(
        &mut self,
        cursor: &mut UnavailablePgReconciliationCursor,
        now_ms: u64,
    ) -> Result<UnavailablePgReconciliationPollBatch, ControlPlaneError> {
        self.poll_unavailable_pg_reconciliation_batch(cursor, now_ms)
    }

    fn complete_reconciliation_batch(
        &mut self,
        work: &[UnavailablePgReconciliationWork],
        now_ms: u64,
    ) -> Result<UnavailablePgReconciliationCompletionBatch, ControlPlaneError> {
        self.complete_unavailable_pg_reconciliation_batch(work, now_ms)
    }

    fn authorize_staging_batch(
        &mut self,
        requests: &[UnavailablePgStagingIntentAuthorizationRequest],
    ) -> Result<ClusterControlSnapshot, ControlPlaneError> {
        self.authorize_unavailable_pg_staging_intents_batch(requests)
    }

    fn install_staged_batch(
        &mut self,
        requests: &[UnavailablePgTransitionInstallRequest],
        expected_destination_epoch: ClusterEpoch,
    ) -> Result<ClusterControlSnapshot, ControlPlaneError> {
        self.install_unavailable_pg_placement_transitions_batch(
            requests,
            expected_destination_epoch,
        )
    }

    fn finalize_staging_generation(
        &mut self,
        cleanup: FinalizeMetadataTransferStagingGenerationRequest,
    ) -> Result<ClusterControlSnapshot, ControlPlaneError> {
        self.finalize_metadata_transfer_staging_generation(cleanup)
    }

    fn maintain_staging_evidence(
        &mut self,
        cursor: &mut MetadataTransferStagingMaintenanceCursor,
    ) -> Result<bool, ControlPlaneError> {
        self.maintain_metadata_transfer_staging_evidence_once(cursor)
    }
}

type ReconciliationRetryKey = (PgId, ClusterEpoch, UnavailablePgReconciliationStage);

fn reconciliation_retry_key(work: &UnavailablePgReconciliationWork) -> ReconciliationRetryKey {
    (work.pg_id(), work.transition_epoch(), work.stage())
}

pub struct UnavailablePgReconciliationWorker {
    work_tx: SyncSender<TransferJob>,
    completion_rx: Receiver<TransferWorkerEvent>,
    staged_protocol: bool,
    cursor: UnavailablePgReconciliationCursor,
    staging_maintenance_cursor: MetadataTransferStagingMaintenanceCursor,
    in_flight: BTreeMap<PgId, UnavailablePgReconciliationWork>,
    pending_transfers: VecDeque<TransferJob>,
    prepared_for_authorization: Vec<PreparedUnavailablePgMetadataTransfer>,
    staged_for_install: Vec<StagedUnavailablePgMetadataTransfer>,
    install_evidence_wait_started_at: Option<Instant>,
    ready_for_activation: Vec<ActivationOwner>,
    tombstoned_for_finalization: Vec<TombstonedUnavailablePgMetadataTransfer>,
    authority_retry_not_before: Instant,
    maintenance_retry_not_before: Instant,
    deferred: BTreeMap<ReconciliationRetryKey, Instant>,
    blocked: BTreeSet<ReconciliationRetryKey>,
    retry_backoff: Duration,
    last_diagnostic: Option<String>,
    last_maintenance_diagnostic: Option<String>,
    #[cfg(test)]
    next_authorization_response_error: Option<ControlPlaneError>,
    #[cfg(test)]
    next_install_response_error: Option<ControlPlaneError>,
    #[cfg(test)]
    next_install_preparation_rejection: Option<ControlPlaneError>,
    #[cfg(test)]
    next_finalization_error: Option<ControlPlaneError>,
}

impl UnavailablePgReconciliationWorker {
    #[must_use]
    pub fn spawn(admin: LivePgMetadataTransferAdmin) -> Self {
        Self::spawn_with_executor(
            TransferExecutor::Staged(Arc::new(admin)),
            RETRY_BACKOFF,
            true,
        )
    }

    #[allow(dead_code)] // Used only by owner-local unit-test executors.
    fn spawn_with_transfer(
        transfer: impl Fn(&UnavailablePgReconciliationWork) -> Result<(), ReconciliationTransferError>
            + Send
            + Sync
            + 'static,
        retry_backoff: Duration,
    ) -> Self {
        Self::spawn_with_executor(
            TransferExecutor::Legacy(Arc::new(transfer)),
            retry_backoff,
            false,
        )
    }

    fn spawn_with_executor(
        executor: TransferExecutor,
        retry_backoff: Duration,
        staged_protocol: bool,
    ) -> Self {
        let (work_tx, work_rx) = mpsc::sync_channel::<TransferJob>(TRANSFER_WORKER_COUNT);
        let (completion_tx, completion_rx) = mpsc::channel();
        let work_rx = Arc::new(Mutex::new(work_rx));
        let executor = Arc::new(executor);
        for worker_index in 0..TRANSFER_WORKER_COUNT {
            let work_rx = Arc::clone(&work_rx);
            let completion_tx = completion_tx.clone();
            let executor = Arc::clone(&executor);
            thread::Builder::new()
                .name(format!("unavailable-pg-reconciler-{worker_index}"))
                .spawn(move || loop {
                    let job = {
                        let receiver = work_rx
                            .lock()
                            .expect("unavailable PG reconciliation work queue poisoned");
                        receiver.recv()
                    };
                    let Ok(job) = job else {
                        break;
                    };
                    let work = job.work().clone();
                    let metric_stage = job.metric_stage();
                    let started_at = Instant::now();
                    let result = panic::catch_unwind(AssertUnwindSafe(|| executor.execute(job)));
                    let panicked = result.is_err();
                    let event = match result {
                        Ok(result) => {
                            let outcome = match &result {
                                Ok(_) => observability::UnavailablePgWorkerStageOutcome::Succeeded,
                                Err(ReconciliationTransferError::Retryable(_)) => {
                                    observability::UnavailablePgWorkerStageOutcome::Deferred
                                }
                                Err(ReconciliationTransferError::Fatal(_)) => {
                                    observability::UnavailablePgWorkerStageOutcome::Fatal
                                }
                            };
                            observability::record_unavailable_pg_worker_stage(
                                metric_stage,
                                outcome,
                                started_at.elapsed(),
                            );
                            TransferWorkerEvent::Completed(Box::new(TransferCompletion {
                                work,
                                result,
                            }))
                        }
                        Err(_) => {
                            record_worker_stage_outcome(
                                metric_stage,
                                started_at,
                                observability::UnavailablePgWorkerStageOutcome::Fatal,
                            );
                            TransferWorkerEvent::Panicked
                        }
                    };
                    if completion_tx.send(event).is_err() || panicked {
                        break;
                    }
                })
                .expect("failed to spawn unavailable PG reconciliation worker");
        }
        drop(completion_tx);
        let worker = Self {
            work_tx,
            completion_rx,
            staged_protocol,
            cursor: UnavailablePgReconciliationCursor::start(),
            staging_maintenance_cursor: MetadataTransferStagingMaintenanceCursor::start(),
            in_flight: BTreeMap::new(),
            pending_transfers: VecDeque::new(),
            prepared_for_authorization: Vec::new(),
            staged_for_install: Vec::new(),
            install_evidence_wait_started_at: None,
            ready_for_activation: Vec::new(),
            tombstoned_for_finalization: Vec::new(),
            authority_retry_not_before: Instant::now(),
            maintenance_retry_not_before: Instant::now(),
            deferred: BTreeMap::new(),
            blocked: BTreeSet::new(),
            retry_backoff,
            last_diagnostic: None,
            last_maintenance_diagnostic: None,
            #[cfg(test)]
            next_authorization_response_error: None,
            #[cfg(test)]
            next_install_response_error: None,
            #[cfg(test)]
            next_install_preparation_rejection: None,
            #[cfg(test)]
            next_finalization_error: None,
        };
        worker.record_queue_metrics();
        worker
    }

    pub fn poll_single_authority(
        &mut self,
        authority: &mut SingleAuthorityControlPlane<FileControlPlaneStore>,
        now_ms: u64,
    ) -> Result<(), ControlPlaneError> {
        self.poll(authority, now_ms)
    }

    pub fn poll_raft(
        &mut self,
        authority: &mut ControlPlaneRaftAuthorityHost,
        now_ms: u64,
    ) -> Result<(), ControlPlaneError> {
        self.poll(authority, now_ms)
    }

    /// Observe transfer completion and fail-stop worker loss independently of authority role.
    pub fn observe_transfer_workers(&mut self) {
        self.receive_completion();
        self.record_queue_metrics();
    }

    fn record_queue_metrics(&self) {
        let prepared_artifact_bytes = self
            .prepared_for_authorization
            .iter()
            .map(PreparedUnavailablePgMetadataTransfer::artifact_length)
            .fold(0_u64, u64::saturating_add);
        let staged_install_bytes = self
            .staged_for_install
            .iter()
            .map(StagedUnavailablePgMetadataTransfer::artifact_length)
            .fold(0_u64, u64::saturating_add);
        observability::record_unavailable_pg_worker_queue(
            observability::UnavailablePgWorkerQueueMetricSnapshot {
                pending_transfer_depth: u64::try_from(self.pending_transfers.len())
                    .unwrap_or(u64::MAX),
                in_flight_transfer_depth: u64::try_from(self.in_flight.len()).unwrap_or(u64::MAX),
                prepared_artifact_depth: u64::try_from(self.prepared_for_authorization.len())
                    .unwrap_or(u64::MAX),
                prepared_artifact_bytes,
                staged_install_depth: u64::try_from(self.staged_for_install.len())
                    .unwrap_or(u64::MAX),
                staged_install_bytes,
                ready_activation_depth: u64::try_from(self.ready_for_activation.len())
                    .unwrap_or(u64::MAX),
                pending_finalization_depth: u64::try_from(self.tombstoned_for_finalization.len())
                    .unwrap_or(u64::MAX),
                deferred_depth: u64::try_from(self.deferred.len()).unwrap_or(u64::MAX),
                blocked_depth: u64::try_from(self.blocked.len()).unwrap_or(u64::MAX),
            },
        );
    }

    #[cfg(test)]
    pub(crate) fn retained_test_state(&self) -> (usize, usize, usize, Option<&str>) {
        (
            self.in_flight.len(),
            self.pending_transfers.len(),
            self.blocked.len(),
            self.last_diagnostic.as_deref(),
        )
    }

    #[cfg(test)]
    pub(crate) fn staged_install_depth_for_test(&self) -> usize {
        self.staged_for_install.len()
    }

    #[cfg(test)]
    pub(crate) fn expire_install_evidence_wait_for_test(&mut self) {
        assert!(
            self.install_evidence_wait_started_at.is_some(),
            "test must enter the evidence wait before expiring it"
        );
        self.install_evidence_wait_started_at =
            Some(Instant::now() - STAGING_EVIDENCE_INSTALL_WAIT);
    }

    #[cfg(test)]
    pub(crate) fn install_evidence_wait_is_pending_for_test(&self) -> bool {
        self.install_evidence_wait_started_at.is_some()
    }

    #[cfg(test)]
    pub(crate) fn foreground_owns_pg_for_test(&self, pg_id: PgId) -> bool {
        self.in_flight.contains_key(&pg_id)
            || self
                .pending_transfers
                .iter()
                .any(|job| job.work().pg_id() == pg_id)
            || self
                .prepared_for_authorization
                .iter()
                .any(|owner| owner.work().pg_id() == pg_id)
            || self
                .staged_for_install
                .iter()
                .any(|owner| owner.work().pg_id() == pg_id)
            || self
                .ready_for_activation
                .iter()
                .any(|owner| owner.work().pg_id() == pg_id)
            || self
                .tombstoned_for_finalization
                .iter()
                .any(|owner| owner.work().pg_id() == pg_id)
    }

    #[cfg(test)]
    pub(crate) fn pg_is_deferred_for_test(&self, pg_id: PgId) -> bool {
        self.deferred
            .keys()
            .any(|(candidate, _, _)| *candidate == pg_id)
    }

    #[cfg(test)]
    pub(crate) fn fail_next_authorization_response_for_test(&mut self) {
        self.next_authorization_response_error = Some(ControlPlaneError::RpcUnconfirmed {
            message: "injected staging authorization response loss".to_owned(),
        });
    }

    #[cfg(test)]
    pub(crate) fn reject_next_authorization_response_for_test(&mut self) {
        self.next_authorization_response_error = Some(ControlPlaneError::CommandDecode {
            message: "injected definitive staging authorization rejection".to_owned(),
        });
    }

    #[cfg(test)]
    pub(crate) fn fail_next_install_response_for_test(&mut self) {
        self.next_install_response_error = Some(ControlPlaneError::RpcUnconfirmed {
            message: "injected destination installation response loss".to_owned(),
        });
    }

    #[cfg(test)]
    pub(crate) fn install_response_failure_is_pending_for_test(&self) -> bool {
        self.next_install_response_error.is_some()
    }

    #[cfg(test)]
    pub(crate) fn reject_next_install_preparation_for_test(&mut self) {
        self.next_install_preparation_rejection = Some(ControlPlaneError::CommandDecode {
            message: "injected stale destination installation member".to_owned(),
        });
    }

    #[cfg(test)]
    pub(crate) fn defer_next_finalization_for_test(&mut self) {
        self.next_finalization_error = Some(ControlPlaneError::CommandDecode {
            message: "injected missing staging tombstone evidence".to_owned(),
        });
    }

    fn record_diagnostic(&mut self, diagnostic: String) {
        if self.last_diagnostic.as_deref() != Some(&diagnostic) {
            eprintln!("unavailable PG reconciliation deferred: {diagnostic}");
            self.last_diagnostic = Some(diagnostic);
        }
    }

    fn clear_diagnostic(&mut self) {
        self.last_diagnostic = None;
    }

    fn dispatch(&mut self, job: TransferJob) {
        let work = job.work().clone();
        let pg_id = work.pg_id();
        assert!(
            !self.in_flight.contains_key(&pg_id),
            "unavailable PG reconciliation dispatched duplicate PG work"
        );
        match self.work_tx.try_send(job) {
            Ok(()) => {
                self.in_flight.insert(pg_id, work);
                self.clear_diagnostic();
            }
            Err(TrySendError::Full(_)) => {
                panic!("unavailable PG reconciliation transfer queue exceeded its bound")
            }
            Err(TrySendError::Disconnected(_)) => {
                panic!("unavailable PG reconciliation transfer workers terminated unexpectedly")
            }
        }
    }

    fn dispatch_pending_transfers(&mut self) {
        while self.in_flight.len() < TRANSFER_WORKER_COUNT {
            let Some(job) = self.pending_transfers.pop_front() else {
                break;
            };
            self.dispatch(job);
        }
    }

    fn defer(&mut self, work: &UnavailablePgReconciliationWork) {
        self.deferred.insert(
            reconciliation_retry_key(work),
            Instant::now() + self.retry_backoff,
        );
    }

    fn candidate_is_deferred_or_blocked(&mut self, work: &UnavailablePgReconciliationWork) -> bool {
        let key = reconciliation_retry_key(work);
        if self.blocked.contains(&key) {
            return true;
        }
        let Some(retry_not_before) = self.deferred.get(&key).copied() else {
            return false;
        };
        if Instant::now() < retry_not_before {
            return true;
        }
        self.deferred.remove(&key);
        false
    }

    fn clear_work_retry_state(&mut self, work: &UnavailablePgReconciliationWork) {
        let pg_id = work.pg_id();
        let transition_epoch = work.transition_epoch();
        self.deferred
            .retain(|(candidate_pg_id, candidate_epoch, _), _| {
                *candidate_pg_id != pg_id || *candidate_epoch != transition_epoch
            });
        self.blocked
            .retain(|(candidate_pg_id, candidate_epoch, _)| {
                *candidate_pg_id != pg_id || *candidate_epoch != transition_epoch
            });
    }

    fn select_reconciliation_work(
        &mut self,
        primary: Vec<UnavailablePgReconciliationWork>,
        cleanup_fallbacks: Vec<UnavailablePgReconciliationWork>,
        rejected_pg_ids: &[PgId],
    ) -> Vec<UnavailablePgReconciliationWork> {
        let mut cleanup_fallbacks = cleanup_fallbacks
            .into_iter()
            .map(|work| (work.pg_id(), work))
            .collect::<BTreeMap<_, _>>();
        let mut selected = Vec::with_capacity(primary.len() + rejected_pg_ids.len());
        for primary in primary {
            if self.candidate_is_deferred_or_blocked(&primary) {
                if let Some(cleanup) = cleanup_fallbacks.remove(&primary.pg_id()) {
                    if !self.candidate_is_deferred_or_blocked(&cleanup) {
                        selected.push(cleanup);
                    }
                }
            } else {
                cleanup_fallbacks.remove(&primary.pg_id());
                selected.push(primary);
            }
        }
        for pg_id in rejected_pg_ids {
            if let Some(cleanup) = cleanup_fallbacks.remove(pg_id) {
                if !self.candidate_is_deferred_or_blocked(&cleanup) {
                    selected.push(cleanup);
                }
            }
        }
        selected
    }

    fn receive_completion(&mut self) {
        loop {
            let event = match self.completion_rx.try_recv() {
                Ok(event) => event,
                Err(TryRecvError::Empty) => return,
                Err(TryRecvError::Disconnected) => {
                    panic!("unavailable PG reconciliation transfer workers terminated unexpectedly")
                }
            };
            let completion = match event {
                TransferWorkerEvent::Completed(completion) => *completion,
                TransferWorkerEvent::Panicked => {
                    panic!("unavailable PG reconciliation transfer worker terminated unexpectedly")
                }
            };
            let retained = self
                .in_flight
                .remove(&completion.work.pg_id())
                .expect("transfer worker returned work that was not in flight");
            assert_eq!(
                retained, completion.work,
                "transfer worker returned a result for a different transition"
            );
            match completion.result {
                Ok(TransferOutcome::LegacyReady) => {
                    self.ready_for_activation
                        .push(ActivationOwner::Legacy(completion.work));
                    self.clear_diagnostic();
                }
                Ok(TransferOutcome::Prepared(prepared)) => {
                    self.prepared_for_authorization.push(prepared);
                    self.clear_diagnostic();
                }
                Ok(TransferOutcome::Authorized(authorized)) => {
                    self.pending_transfers
                        .push_front(TransferJob::Stage(authorized));
                    self.clear_diagnostic();
                }
                Ok(TransferOutcome::ReadyInstall(staged)) => {
                    self.staged_for_install.push(staged);
                    self.clear_diagnostic();
                }
                Ok(TransferOutcome::ReadyImport(staged)) => {
                    self.pending_transfers
                        .push_front(TransferJob::Import(staged));
                    self.clear_diagnostic();
                }
                Ok(TransferOutcome::ReadyCleanup(staged, snapshot)) => {
                    self.pending_transfers
                        .push_front(TransferJob::Tombstone(staged, snapshot));
                    self.clear_diagnostic();
                }
                Ok(TransferOutcome::Imported(staged)) => {
                    self.ready_for_activation
                        .push(ActivationOwner::Staged(Box::new(staged)));
                    self.clear_diagnostic();
                }
                Ok(TransferOutcome::Tombstoned(tombstoned)) => {
                    self.tombstoned_for_finalization.push(tombstoned);
                    self.clear_diagnostic();
                }
                Err(error) => {
                    let fatal = error.is_fatal();
                    let diagnostic = error.diagnostic();
                    if fatal {
                        eprintln!(
                            "unavailable PG reconciliation blocked by fatal transfer error: {diagnostic}"
                        );
                        self.blocked
                            .insert(reconciliation_retry_key(&completion.work));
                    } else {
                        self.record_diagnostic(diagnostic);
                        self.defer(&completion.work);
                    }
                }
            }
        }
    }

    fn record_completion_error(
        &mut self,
        work: &UnavailablePgReconciliationWork,
        error: ControlPlaneError,
    ) {
        let diagnostic = error.to_string();
        if reconciliation_completion_error_is_fatal(&error) {
            eprintln!("unavailable PG reconciliation blocked by fatal error: {diagnostic}");
            self.blocked.insert(reconciliation_retry_key(work));
        } else {
            self.record_diagnostic(diagnostic);
            self.defer(work);
        }
    }

    fn complete_ready_batch(
        &mut self,
        authority: &mut impl ReconciliationAuthority,
        mut owners: Vec<ActivationOwner>,
        now_ms: u64,
    ) {
        if owners.is_empty() {
            return;
        }
        owners.sort_by_key(|owner| owner.work().pg_id());
        let work = owners
            .iter()
            .map(|owner| owner.work().clone())
            .collect::<Vec<_>>();
        let started_at = Instant::now();
        let result = authority.complete_reconciliation_batch(&work, now_ms);
        match result {
            Ok(outcome) => {
                let mut metric_outcome = observability::UnavailablePgWorkerStageOutcome::Succeeded;
                let mut owners = owners
                    .into_iter()
                    .map(|owner| (owner.work().pg_id(), owner))
                    .collect::<BTreeMap<_, _>>();
                for work in outcome.completed {
                    self.clear_work_retry_state(&work);
                    match owners.remove(&work.pg_id()) {
                        Some(ActivationOwner::Staged(staged)) => {
                            self.pending_transfers.push_back(TransferJob::Tombstone(
                                (*staged).into_cleanup(),
                                Box::new(outcome.snapshot.clone()),
                            ));
                        }
                        Some(ActivationOwner::Legacy(_)) => {}
                        None => panic!(
                            "activation batch completed work without retained transfer ownership"
                        ),
                    }
                }
                for (work, error) in outcome.rejected {
                    let owner = owners.remove(&work.pg_id());
                    if reconciliation_completion_error_is_fatal(&error) {
                        metric_outcome = observability::UnavailablePgWorkerStageOutcome::Fatal;
                        self.record_completion_error(&work, error);
                    } else {
                        if metric_outcome != observability::UnavailablePgWorkerStageOutcome::Fatal {
                            metric_outcome =
                                observability::UnavailablePgWorkerStageOutcome::Deferred;
                        }
                        self.record_diagnostic(error.to_string());
                        if let Some(owner) = owner {
                            match owner {
                                ActivationOwner::Legacy(work) => self.defer(&work),
                                ActivationOwner::Staged(staged) => self.defer(staged.work()),
                            }
                        }
                    }
                }
                if !outcome.rederive.is_empty()
                    && metric_outcome != observability::UnavailablePgWorkerStageOutcome::Fatal
                {
                    metric_outcome = observability::UnavailablePgWorkerStageOutcome::Deferred;
                }
                for work in outcome.rederive {
                    owners.remove(&work.pg_id());
                    self.defer(&work);
                }
                if !owners.is_empty()
                    && metric_outcome != observability::UnavailablePgWorkerStageOutcome::Fatal
                {
                    metric_outcome = observability::UnavailablePgWorkerStageOutcome::Deferred;
                }
                for (_, owner) in owners {
                    self.defer(owner.work());
                }
                if metric_outcome == observability::UnavailablePgWorkerStageOutcome::Succeeded {
                    self.clear_diagnostic();
                }
                record_worker_stage_outcome(
                    observability::UnavailablePgWorkerStage::Activation,
                    started_at,
                    metric_outcome,
                );
            }
            Err(error) => {
                let diagnostic = error.to_string();
                let fatal = reconciliation_completion_error_is_fatal(&error);
                for owner in owners {
                    let work = owner.work();
                    if fatal {
                        self.blocked.insert(reconciliation_retry_key(work));
                    } else {
                        match owner {
                            ActivationOwner::Legacy(work) => self.defer(&work),
                            ActivationOwner::Staged(staged) => self.defer(staged.work()),
                        }
                    }
                }
                if fatal {
                    eprintln!(
                        "unavailable PG reconciliation batch blocked by fatal error: {diagnostic}"
                    );
                } else {
                    self.record_diagnostic(diagnostic);
                }
                record_worker_stage_outcome(
                    observability::UnavailablePgWorkerStage::Activation,
                    started_at,
                    if fatal {
                        observability::UnavailablePgWorkerStageOutcome::Fatal
                    } else {
                        observability::UnavailablePgWorkerStageOutcome::Deferred
                    },
                );
            }
        }
    }

    fn authorize_prepared_batch(&mut self, authority: &mut impl ReconciliationAuthority) {
        let mut prepared = std::mem::take(&mut self.prepared_for_authorization);
        if prepared.is_empty() {
            return;
        }
        prepared.sort_by_key(|owner| owner.work().pg_id());
        let requests = prepared
            .iter()
            .map(PreparedUnavailablePgMetadataTransfer::authorization_request)
            .collect::<Vec<_>>();
        let started_at = Instant::now();
        let authorization_result = authority.authorize_staging_batch(&requests);
        #[cfg(test)]
        let authorization_result = if authorization_result.is_ok() {
            self.next_authorization_response_error
                .take()
                .map_or(authorization_result, Err)
        } else {
            authorization_result
        };
        record_authority_stage_result(
            observability::UnavailablePgWorkerStage::Authorization,
            started_at,
            &authorization_result,
        );
        match authorization_result {
            Ok(snapshot) => {
                for owner in prepared {
                    let work = owner.work().clone();
                    match owner.bind_committed_authorization(&snapshot) {
                        Ok(authorized) => self
                            .pending_transfers
                            .push_front(TransferJob::Stage(authorized)),
                        Err(error) => self.record_transfer_error(work, error),
                    }
                }
            }
            Err(error) => {
                if reconciliation_completion_error_is_fatal(&error) {
                    self.record_authority_batch_error(
                        prepared.into_iter().map(|owner| owner.work().clone()),
                        error,
                        "staging authorization",
                    );
                } else {
                    for owner in &prepared {
                        self.defer(owner.work());
                    }
                    self.record_diagnostic(format!(
                        "unavailable PG staging authorization deferred: {error}"
                    ));
                }
            }
        }
    }

    fn install_staged_batch(&mut self, authority: &mut impl ReconciliationAuthority) {
        let mut staged = std::mem::take(&mut self.staged_for_install);
        if staged.is_empty() {
            self.install_evidence_wait_started_at = None;
            return;
        }
        staged.sort_by_key(|owner| owner.work().pg_id());
        let snapshot = match authority.reconciliation_snapshot() {
            Ok(snapshot) => snapshot,
            Err(error) => {
                self.install_evidence_wait_started_at = None;
                self.record_authority_batch_error(
                    staged.into_iter().map(|owner| owner.work().clone()),
                    error,
                    "destination installation snapshot",
                );
                return;
            }
        };
        let target_epoch = match snapshot.next_cluster_epoch() {
            Ok(target_epoch) => target_epoch,
            Err(error) => {
                self.install_evidence_wait_started_at = None;
                self.record_authority_batch_error(
                    staged.into_iter().map(|owner| owner.work().clone()),
                    error,
                    "destination installation epoch",
                );
                return;
            }
        };
        let mut ready = Vec::new();
        for owner in staged {
            match snapshot.committed_unavailable_pg_staged_transfer_if_present(owner.work()) {
                Ok(Some(_)) => {
                    self.pending_transfers
                        .push_front(TransferJob::ResumeInstalled(
                            owner.work().clone(),
                            Box::new(snapshot.clone()),
                        ));
                }
                Ok(None) if owner.target_epoch() == target_epoch => ready.push(owner),
                Ok(None) => {
                    self.pending_transfers
                        .push_front(TransferJob::Rebase(owner, target_epoch));
                }
                Err(error) => self.record_completion_error(owner.work(), error),
            }
        }
        if ready.is_empty() {
            self.install_evidence_wait_started_at = None;
            return;
        }
        let requests = ready
            .iter()
            .map(StagedUnavailablePgMetadataTransfer::install_request)
            .collect::<Vec<_>>();
        let prepared = match snapshot
            .prepare_unavailable_pg_placement_install_batch(&requests, target_epoch)
        {
            Ok(prepared) => prepared,
            Err(error) => {
                self.install_evidence_wait_started_at = None;
                self.record_authority_batch_error(
                    ready.into_iter().map(|owner| owner.work().clone()),
                    error,
                    "destination installation preparation",
                );
                return;
            }
        };
        #[cfg(test)]
        let mut prepared = prepared;
        #[cfg(test)]
        if let Some(error) = self.next_install_preparation_rejection.take() {
            if !prepared.included.is_empty() {
                let rejected = prepared.included.remove(0);
                let mut existing_rejections = std::mem::take(&mut prepared.rejected);
                if prepared.included.is_empty() {
                    prepared.command = None;
                } else {
                    prepared = snapshot
                        .prepare_unavailable_pg_placement_install_batch(
                            &prepared.included,
                            target_epoch,
                        )
                        .expect("test install-preparation remainder must remain valid");
                }
                existing_rejections.append(&mut prepared.rejected);
                prepared.rejected = existing_rejections;
                prepared.rejected.push((rejected, error));
            }
        }
        let mut owners = ready
            .into_iter()
            .map(|owner| (owner.work().pg_id(), owner))
            .collect::<BTreeMap<_, _>>();
        let mut awaiting_publication = Vec::new();
        for (request, error) in prepared.rejected {
            let pg_id = request.unavailable_transition.pg_id();
            let owner = owners
                .remove(&pg_id)
                .expect("install preparation rejected an unknown staged owner");
            if !reconciliation_completion_error_is_fatal(&error)
                && snapshot.unavailable_pg_install_awaits_publication(&request)
            {
                awaiting_publication.push(owner);
            } else if reconciliation_completion_error_is_fatal(&error) {
                self.record_completion_error(owner.work(), error);
            } else {
                self.record_diagnostic(error.to_string());
                self.defer(owner.work());
            }
        }
        let included_pg_ids = prepared
            .included
            .iter()
            .map(|request| request.unavailable_transition.pg_id())
            .collect::<Vec<_>>();
        let included = included_pg_ids
            .iter()
            .map(|pg_id| {
                owners
                    .remove(pg_id)
                    .expect("install preparation included an unknown staged owner")
            })
            .collect::<Vec<_>>();
        self.staged_for_install.extend(owners.into_values());
        if !awaiting_publication.is_empty() {
            let started_at = self
                .install_evidence_wait_started_at
                .get_or_insert_with(Instant::now);
            if started_at.elapsed() < STAGING_EVIDENCE_INSTALL_WAIT {
                self.staged_for_install.extend(included);
                self.staged_for_install.extend(awaiting_publication);
                return;
            }
            for owner in awaiting_publication {
                self.defer(owner.work());
            }
        }
        self.install_evidence_wait_started_at = None;
        if prepared.command.is_none() {
            return;
        }
        let started_at = Instant::now();
        let install_result = authority.install_staged_batch(&prepared.included, target_epoch);
        #[cfg(test)]
        let install_result = if install_result.is_ok() {
            self.next_install_response_error
                .take()
                .map_or(install_result, Err)
        } else {
            install_result
        };
        record_authority_stage_result(
            observability::UnavailablePgWorkerStage::Install,
            started_at,
            &install_result,
        );
        match install_result {
            Ok(_) => {
                for owner in included {
                    self.pending_transfers
                        .push_front(TransferJob::Import(owner));
                }
            }
            Err(error) => {
                if reconciliation_completion_error_is_fatal(&error) {
                    self.record_authority_batch_error(
                        included.into_iter().map(|owner| owner.work().clone()),
                        error,
                        "destination installation",
                    );
                } else {
                    for owner in &included {
                        self.defer(owner.work());
                    }
                    self.record_diagnostic(format!(
                        "unavailable PG destination installation deferred: {error}"
                    ));
                }
            }
        }
    }

    fn finalize_tombstoned(&mut self, authority: &mut impl ReconciliationAuthority) {
        let tombstoned = std::mem::take(&mut self.tombstoned_for_finalization);
        for owner in tombstoned {
            let work = owner.work().clone();
            let started_at = Instant::now();
            #[cfg(test)]
            let result = if let Some(error) = self.next_finalization_error.take() {
                Err(error)
            } else {
                authority.finalize_staging_generation(owner.cleanup_request().clone())
            };
            #[cfg(not(test))]
            let result = authority.finalize_staging_generation(owner.cleanup_request().clone());
            record_authority_stage_result(
                observability::UnavailablePgWorkerStage::Finalization,
                started_at,
                &result,
            );
            match result {
                Ok(_) => {
                    self.clear_work_retry_state(&work);
                    self.clear_diagnostic();
                }
                Err(error) if reconciliation_completion_error_is_fatal(&error) => {
                    eprintln!(
                        "unavailable PG staging finalization blocked by fatal error: {error}"
                    );
                    self.blocked.insert(reconciliation_retry_key(&work));
                }
                Err(error) => {
                    self.record_diagnostic(error.to_string());
                    self.defer(&work);
                }
            }
        }
    }

    fn record_transfer_error(
        &mut self,
        work: UnavailablePgReconciliationWork,
        error: crate::LivePgMetadataTransferError,
    ) {
        let error = reconciliation_transfer_error(error);
        let fatal = error.is_fatal();
        let diagnostic = error.diagnostic();
        if fatal {
            eprintln!(
                "unavailable PG reconciliation blocked by fatal transfer error: {diagnostic}"
            );
            self.blocked.insert(reconciliation_retry_key(&work));
        } else {
            self.record_diagnostic(diagnostic);
            self.defer(&work);
        }
    }

    fn record_authority_batch_error(
        &mut self,
        work: impl IntoIterator<Item = UnavailablePgReconciliationWork>,
        error: ControlPlaneError,
        operation: &str,
    ) {
        let fatal = reconciliation_completion_error_is_fatal(&error);
        let diagnostic = format!("unavailable PG {operation} deferred: {error}");
        for work in work {
            if fatal {
                self.blocked.insert(reconciliation_retry_key(&work));
            } else {
                self.defer(&work);
            }
        }
        if fatal {
            eprintln!("unavailable PG {operation} blocked by fatal error: {error}");
        } else {
            self.record_diagnostic(diagnostic);
        }
    }

    fn poll(
        &mut self,
        authority: &mut impl ReconciliationAuthority,
        now_ms: u64,
    ) -> Result<(), ControlPlaneError> {
        self.poll_reconciliation(authority, now_ms);
        let result = self.poll_staging_maintenance(authority);
        self.record_queue_metrics();
        result
    }

    fn poll_reconciliation(&mut self, authority: &mut impl ReconciliationAuthority, now_ms: u64) {
        self.observe_transfer_workers();
        if Instant::now() < self.authority_retry_not_before {
            return;
        }
        // Drain the whole selected page before advancing an epoch-bound stage.
        // Otherwise the four transfer workers turn one page into four-member
        // install batches and every later member must rebase its proof again.
        self.dispatch_pending_transfers();
        if !self.in_flight.is_empty() || !self.pending_transfers.is_empty() {
            return;
        }
        if !self.tombstoned_for_finalization.is_empty() {
            self.finalize_tombstoned(authority);
            return;
        }
        if !self.prepared_for_authorization.is_empty() {
            self.authorize_prepared_batch(authority);
            self.dispatch_pending_transfers();
            return;
        }
        if !self.staged_for_install.is_empty() {
            self.install_staged_batch(authority);
            self.dispatch_pending_transfers();
            return;
        }
        if !self.ready_for_activation.is_empty() {
            let ready = std::mem::take(&mut self.ready_for_activation);
            self.complete_ready_batch(authority, ready, now_ms);
            return;
        }
        match authority.poll_reconciliation_batch(&mut self.cursor, now_ms) {
            Ok(batch) => {
                let had_rejections = !batch.rejected.is_empty();
                let mut rejected_pg_ids = Vec::with_capacity(batch.rejected.len());
                for (pg_id, error) in batch.rejected {
                    rejected_pg_ids.push(pg_id);
                    self.record_diagnostic(format!(
                        "PG {} begin candidate rejected: {error}",
                        pg_id.get()
                    ));
                }
                let work = self.select_reconciliation_work(
                    batch.work,
                    batch.cleanup_fallbacks,
                    &rejected_pg_ids,
                );
                let had_work = !work.is_empty();
                let snapshot = if self.staged_protocol && had_work {
                    match authority.reconciliation_snapshot() {
                        Ok(snapshot) => Some(snapshot),
                        Err(error) => {
                            self.record_diagnostic(error.to_string());
                            self.authority_retry_not_before = Instant::now() + self.retry_backoff;
                            return;
                        }
                    }
                } else {
                    None
                };
                for work in work {
                    if self.staged_protocol {
                        let snapshot = snapshot
                            .as_ref()
                            .expect("staged reconciliation snapshot loaded for nonempty work");
                        match work.stage() {
                            UnavailablePgReconciliationStage::PayloadReadiness => {
                                self.pending_transfers
                                    .push_back(TransferJob::ResumeInstalled(
                                        work,
                                        Box::new((*snapshot).clone()),
                                    ));
                            }
                            UnavailablePgReconciliationStage::MetadataTransfer => {
                                if snapshot
                                    .committed_unavailable_pg_staging_request(&work)
                                    .is_ok()
                                {
                                    self.pending_transfers.push_back(
                                        TransferJob::ResumeAuthorized(
                                            work,
                                            Box::new((*snapshot).clone()),
                                        ),
                                    );
                                } else {
                                    self.pending_transfers.push_back(TransferJob::Prepare(work));
                                }
                            }
                            UnavailablePgReconciliationStage::StagingCleanup => {
                                self.pending_transfers.push_back(TransferJob::ResumeCleanup(
                                    work,
                                    Box::new((*snapshot).clone()),
                                ));
                            }
                        }
                    } else {
                        match work.stage() {
                            UnavailablePgReconciliationStage::PayloadReadiness => {
                                self.ready_for_activation
                                    .push(ActivationOwner::Legacy(work));
                            }
                            UnavailablePgReconciliationStage::MetadataTransfer => {
                                self.pending_transfers.push_back(TransferJob::Legacy(work));
                            }
                            UnavailablePgReconciliationStage::StagingCleanup => {
                                panic!("legacy reconciliation cannot own staged cleanup")
                            }
                        }
                    }
                }
                self.dispatch_pending_transfers();
                if self.in_flight.is_empty() && self.pending_transfers.is_empty() {
                    let ready = std::mem::take(&mut self.ready_for_activation);
                    self.complete_ready_batch(authority, ready, now_ms);
                }
                if had_rejections && !had_work {
                    self.authority_retry_not_before = Instant::now() + self.retry_backoff;
                }
            }
            Err(error) => {
                self.record_diagnostic(error.to_string());
                self.authority_retry_not_before = Instant::now() + self.retry_backoff;
            }
        }
    }

    fn poll_staging_maintenance(
        &mut self,
        authority: &mut impl ReconciliationAuthority,
    ) -> Result<(), ControlPlaneError> {
        if Instant::now() < self.maintenance_retry_not_before {
            return Ok(());
        }
        match authority.maintain_staging_evidence(&mut self.staging_maintenance_cursor) {
            Ok(_) => {
                self.last_maintenance_diagnostic = None;
                Ok(())
            }
            Err(error) if staging_maintenance_error_is_retryable(&error) => {
                let diagnostic = error.to_string();
                if self.last_maintenance_diagnostic.as_deref() != Some(&diagnostic) {
                    eprintln!("metadata-transfer staging maintenance deferred: {diagnostic}");
                    self.last_maintenance_diagnostic = Some(diagnostic);
                }
                self.maintenance_retry_not_before = Instant::now() + self.retry_backoff;
                Ok(())
            }
            Err(error) => Err(error),
        }
    }
}

fn staging_maintenance_error_is_retryable(error: &ControlPlaneError) -> bool {
    error.is_retryable_staging_evidence_publication_error()
        || matches!(error, ControlPlaneError::RpcUnconfirmed { .. })
        || error.is_retryable_openraft_leadership_error()
}

fn reconciliation_completion_error_is_fatal(error: &ControlPlaneError) -> bool {
    matches!(
        error,
        ControlPlaneError::Parse { .. } | ControlPlaneError::AuthorityClockCheckpoint { .. }
    ) || matches!(
        error,
        ControlPlaneError::RpcProtocol { diagnostic }
            if !diagnostic.is_truncated_frame_marker()
    ) || matches!(
        error,
        ControlPlaneError::SnapshotDecode { .. }
            | ControlPlaneError::SnapshotInvariantViolation { .. }
            | ControlPlaneError::ControlPlaneLogIndexMismatch { .. }
            | ControlPlaneError::ControlPlaneLogIndexOverflow { .. }
            | ControlPlaneError::ControlPlaneSnapshotMissingLogId { .. }
            | ControlPlaneError::ControlPlaneSnapshotLogIndexRegression { .. }
            | ControlPlaneError::ControlPlaneSnapshotLogTermMismatch { .. }
            | ControlPlaneError::ControlPlaneLogTermRegression { .. }
            | ControlPlaneError::DurabilityFailure { .. }
            | ControlPlaneError::InvariantFailure { .. }
            | ControlPlaneError::StaticTopologyFailure { .. }
            | ControlPlaneError::InvalidState { .. }
            | ControlPlaneError::InvalidInitialTopology { .. }
            | ControlPlaneError::OpenRaftOperation {
                kind: crate::control_plane::ControlPlaneRaftOperationErrorKind::Fatal,
                ..
            }
    )
}

#[cfg(test)]
mod tests {
    use std::collections::VecDeque;
    use std::sync::{Arc, Condvar, Mutex};

    use super::*;
    use crate::NodeId;

    #[derive(Default)]
    struct FakeAuthority {
        candidates: VecDeque<UnavailablePgReconciliationWork>,
        poll_batch_size: usize,
        poll_count: usize,
        maintenance_count: usize,
        maintenance_results: VecDeque<Result<bool, ControlPlaneError>>,
        published: Vec<UnavailablePgReconciliationWork>,
        published_batches: Vec<Vec<UnavailablePgReconciliationWork>>,
        publish_results:
            VecDeque<Result<UnavailablePgReconciliationCompletionBatch, ControlPlaneError>>,
    }

    #[derive(Default)]
    struct TransferPoolGate {
        state: Mutex<(usize, usize, bool)>,
        changed: Condvar,
    }

    impl TransferPoolGate {
        fn release(&self) {
            let mut state = self.state.lock().unwrap();
            state.2 = true;
            self.changed.notify_all();
        }
    }

    struct TransferPoolGateRelease(Arc<TransferPoolGate>);

    impl Drop for TransferPoolGateRelease {
        fn drop(&mut self) {
            self.0.release();
        }
    }

    impl ReconciliationAuthority for FakeAuthority {
        fn reconciliation_snapshot(&self) -> Result<ClusterControlSnapshot, ControlPlaneError> {
            Ok(ClusterControlSnapshot::empty())
        }

        fn poll_reconciliation_batch(
            &mut self,
            _cursor: &mut UnavailablePgReconciliationCursor,
            _now_ms: u64,
        ) -> Result<UnavailablePgReconciliationPollBatch, ControlPlaneError> {
            self.poll_count += 1;
            let batch_size = self.poll_batch_size.max(1);
            Ok(UnavailablePgReconciliationPollBatch {
                work: (0..batch_size)
                    .filter_map(|_| self.candidates.pop_front())
                    .collect(),
                cleanup_fallbacks: Vec::new(),
                rejected: Vec::new(),
            })
        }

        fn complete_reconciliation_batch(
            &mut self,
            work: &[UnavailablePgReconciliationWork],
            _now_ms: u64,
        ) -> Result<UnavailablePgReconciliationCompletionBatch, ControlPlaneError> {
            self.published_batches.push(work.to_vec());
            self.published.extend_from_slice(work);
            self.publish_results.pop_front().unwrap_or_else(|| {
                Ok(UnavailablePgReconciliationCompletionBatch {
                    completed: work.to_vec(),
                    rejected: Vec::new(),
                    rederive: Vec::new(),
                    snapshot: ClusterControlSnapshot::empty(),
                })
            })
        }

        fn authorize_staging_batch(
            &mut self,
            _requests: &[UnavailablePgStagingIntentAuthorizationRequest],
        ) -> Result<ClusterControlSnapshot, ControlPlaneError> {
            panic!("legacy reconciliation tests must not authorize staging")
        }

        fn install_staged_batch(
            &mut self,
            _requests: &[UnavailablePgTransitionInstallRequest],
            _expected_destination_epoch: ClusterEpoch,
        ) -> Result<ClusterControlSnapshot, ControlPlaneError> {
            panic!("legacy reconciliation tests must not install staged transfers")
        }

        fn finalize_staging_generation(
            &mut self,
            _cleanup: FinalizeMetadataTransferStagingGenerationRequest,
        ) -> Result<ClusterControlSnapshot, ControlPlaneError> {
            panic!("legacy reconciliation tests must not finalize staging")
        }

        fn maintain_staging_evidence(
            &mut self,
            _cursor: &mut MetadataTransferStagingMaintenanceCursor,
        ) -> Result<bool, ControlPlaneError> {
            self.maintenance_count += 1;
            self.maintenance_results.pop_front().unwrap_or(Ok(false))
        }
    }

    fn work(
        pg_id: u32,
        transition_epoch: u64,
        stage: UnavailablePgReconciliationStage,
    ) -> UnavailablePgReconciliationWork {
        UnavailablePgReconciliationWork::new(
            PgId::new(pg_id),
            ClusterEpoch::new(transition_epoch).unwrap(),
            ClusterEpoch::new(transition_epoch - 1).unwrap(),
            vec![NodeId::new(1), NodeId::new(2), NodeId::new(3)],
            vec![NodeId::new(4), NodeId::new(2), NodeId::new(3)],
            stage,
        )
    }

    fn completed(
        work: Vec<UnavailablePgReconciliationWork>,
    ) -> Result<UnavailablePgReconciliationCompletionBatch, ControlPlaneError> {
        Ok(UnavailablePgReconciliationCompletionBatch {
            completed: work,
            rejected: Vec::new(),
            rederive: Vec::new(),
            snapshot: ClusterControlSnapshot::empty(),
        })
    }

    #[test]
    fn one_in_flight_transfer_suppresses_rediscovery_and_publishes_exact_work() {
        let expected = work(7, 11, UnavailablePgReconciliationStage::MetadataTransfer);
        let transfer_gate = Arc::new((Mutex::new(false), Condvar::new()));
        let (started_tx, started_rx) = mpsc::sync_channel(1);
        let worker_gate = Arc::clone(&transfer_gate);
        let mut worker = UnavailablePgReconciliationWorker::spawn_with_transfer(
            move |work| {
                started_tx.send(work.clone()).unwrap();
                let (lock, ready) = &*worker_gate;
                let mut released = lock.lock().unwrap();
                while !*released {
                    released = ready.wait(released).unwrap();
                }
                Ok(())
            },
            Duration::ZERO,
        );
        let mut authority = FakeAuthority {
            candidates: VecDeque::from([expected.clone()]),
            ..FakeAuthority::default()
        };

        worker.poll(&mut authority, 100).unwrap();
        assert_eq!(
            started_rx.recv_timeout(Duration::from_secs(1)).unwrap(),
            expected
        );
        for _ in 0..100 {
            worker.poll(&mut authority, 101).unwrap();
        }
        assert_eq!(authority.poll_count, 1);
        assert_eq!(authority.maintenance_count, 101);
        assert!(authority.published.is_empty());

        let (lock, ready) = &*transfer_gate;
        *lock.lock().unwrap() = true;
        ready.notify_all();
        for _ in 0..1_000 {
            worker.poll(&mut authority, 102).unwrap();
            if !authority.published.is_empty() {
                break;
            }
            thread::yield_now();
        }
        assert_eq!(authority.published, vec![expected]);
        assert_eq!(authority.poll_count, 1);
    }

    #[test]
    fn transfer_thread_panic_fails_stop_after_dequeue() {
        let expected = work(7, 11, UnavailablePgReconciliationStage::MetadataTransfer);
        let prepare_before = observability::unavailable_pg_worker_stage_metrics_snapshot()
            .into_iter()
            .find(|sample| sample.stage == observability::UnavailablePgWorkerStage::Prepare)
            .unwrap();
        let (started_tx, started_rx) = mpsc::sync_channel(1);
        let mut worker = UnavailablePgReconciliationWorker::spawn_with_transfer(
            move |work| {
                started_tx.send(work.clone()).unwrap();
                panic!("injected transfer worker panic");
            },
            Duration::ZERO,
        );
        let mut authority = FakeAuthority {
            candidates: VecDeque::from([expected.clone()]),
            ..FakeAuthority::default()
        };

        worker.poll(&mut authority, 100).unwrap();
        assert_eq!(
            started_rx.recv_timeout(Duration::from_secs(1)).unwrap(),
            expected
        );

        let deadline = Instant::now() + Duration::from_secs(1);
        loop {
            let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                worker.observe_transfer_workers();
            }));
            if let Err(payload) = result {
                let message = payload
                    .downcast_ref::<&str>()
                    .copied()
                    .or_else(|| payload.downcast_ref::<String>().map(String::as_str))
                    .unwrap_or("non-string panic");
                assert!(
                    message.contains("transfer worker terminated unexpectedly"),
                    "unexpected fail-stop panic: {message}"
                );
                break;
            }
            assert!(
                Instant::now() < deadline,
                "disconnected transfer worker did not fail-stop reconciliation"
            );
            thread::yield_now();
        }

        assert_eq!(authority.poll_count, 1);
        assert_eq!(
            worker.in_flight,
            BTreeMap::from([(expected.pg_id(), expected)])
        );
        assert!(authority.published.is_empty());
        let prepare_after = observability::unavailable_pg_worker_stage_metrics_snapshot()
            .into_iter()
            .find(|sample| sample.stage == observability::UnavailablePgWorkerStage::Prepare)
            .unwrap();
        assert_eq!(prepare_after.total, prepare_before.total + 1);
        assert_eq!(prepare_after.fatal_total, prepare_before.fatal_total + 1);
        assert_eq!(
            prepare_after.succeeded_total,
            prepare_before.succeeded_total
        );
        assert_eq!(prepare_after.deferred_total, prepare_before.deferred_total);
    }

    #[test]
    fn stale_readiness_completion_does_not_publish_into_successor() {
        let stale = work(7, 11, UnavailablePgReconciliationStage::PayloadReadiness);
        let successor = work(7, 12, UnavailablePgReconciliationStage::PayloadReadiness);
        let transfer_count = Arc::new(Mutex::new(0));
        let observed_transfer_count = Arc::clone(&transfer_count);
        let mut worker = UnavailablePgReconciliationWorker::spawn_with_transfer(
            move |_| {
                *observed_transfer_count.lock().unwrap() += 1;
                Ok(())
            },
            Duration::ZERO,
        );
        let mut authority = FakeAuthority {
            candidates: VecDeque::from([stale.clone(), successor.clone()]),
            publish_results: VecDeque::from([
                completed(vec![stale.clone()]),
                completed(vec![successor.clone()]),
            ]),
            ..FakeAuthority::default()
        };

        for now_ms in 100..104 {
            worker.poll(&mut authority, now_ms).unwrap();
        }

        assert_eq!(authority.published, vec![stale, successor]);
        assert_eq!(*transfer_count.lock().unwrap(), 0);
        assert_eq!(authority.poll_count, 4);
    }

    #[test]
    fn retryable_completion_failure_defers_one_pg_and_scans_the_next() {
        let deferred = work(7, 11, UnavailablePgReconciliationStage::PayloadReadiness);
        let successor = work(8, 12, UnavailablePgReconciliationStage::PayloadReadiness);
        let mut worker = UnavailablePgReconciliationWorker::spawn_with_transfer(
            |_| panic!("activation-stage work must not invoke metadata transfer"),
            Duration::from_secs(60),
        );
        let mut authority = FakeAuthority {
            candidates: VecDeque::from([deferred.clone(), successor.clone()]),
            publish_results: VecDeque::from([
                Err(ControlPlaneError::CommandDecode {
                    message: "destination lease changed before activation".to_owned(),
                }),
                completed(vec![successor.clone()]),
            ]),
            ..FakeAuthority::default()
        };

        for now_ms in 100..104 {
            worker.poll(&mut authority, now_ms).unwrap();
        }

        assert_eq!(authority.published, vec![deferred.clone(), successor]);
        assert_eq!(authority.poll_count, 4);
        assert!(worker
            .deferred
            .contains_key(&reconciliation_retry_key(&deferred)));
        assert!(worker.in_flight.is_empty());
    }

    #[test]
    fn deferred_active_successor_selects_retained_cleanup_without_clearing_successor_backoff() {
        let active = work(7, 12, UnavailablePgReconciliationStage::MetadataTransfer);
        let cleanup = work(7, 11, UnavailablePgReconciliationStage::StagingCleanup);
        let mut worker = UnavailablePgReconciliationWorker::spawn_with_transfer(
            |_| Ok(()),
            Duration::from_secs(60),
        );
        worker.defer(&active);

        let selected =
            worker.select_reconciliation_work(vec![active.clone()], vec![cleanup.clone()], &[]);

        assert_eq!(selected, vec![cleanup.clone()]);
        assert!(worker
            .deferred
            .contains_key(&reconciliation_retry_key(&active)));

        worker.deferred.clear();
        worker.blocked.insert(reconciliation_retry_key(&active));
        let selected =
            worker.select_reconciliation_work(vec![active.clone()], vec![cleanup.clone()], &[]);
        assert_eq!(selected, vec![cleanup]);
        assert!(worker.blocked.contains(&reconciliation_retry_key(&active)));
    }

    #[test]
    fn durable_cleanup_supersedes_local_transfer_quarantine_after_leadership_change() {
        let transfer = work(7, 11, UnavailablePgReconciliationStage::MetadataTransfer);
        let cleanup = work(7, 11, UnavailablePgReconciliationStage::StagingCleanup);
        let mut worker = UnavailablePgReconciliationWorker::spawn_with_transfer(
            |_| Ok(()),
            Duration::from_secs(60),
        );
        worker.blocked.insert(reconciliation_retry_key(&transfer));

        // Another leader completed this exact transition. Rediscovery must follow
        // the durable cleanup phase rather than the stale local transfer failure.
        let selected = worker.select_reconciliation_work(vec![cleanup.clone()], Vec::new(), &[]);

        assert_eq!(selected, vec![cleanup.clone()]);
        assert!(worker
            .blocked
            .contains(&reconciliation_retry_key(&transfer)));
        assert!(!worker.blocked.contains(&reconciliation_retry_key(&cleanup)));
    }

    #[test]
    fn fatal_completion_failure_blocks_only_its_exact_transition() {
        let blocked = work(7, 11, UnavailablePgReconciliationStage::PayloadReadiness);
        let successor = work(8, 12, UnavailablePgReconciliationStage::PayloadReadiness);
        let mut worker = UnavailablePgReconciliationWorker::spawn_with_transfer(
            |_| panic!("activation-stage work must not invoke metadata transfer"),
            Duration::ZERO,
        );
        let mut authority = FakeAuthority {
            candidates: VecDeque::from([blocked.clone(), successor.clone(), blocked.clone()]),
            publish_results: VecDeque::from([
                Err(ControlPlaneError::durability_failure(
                    "injected reconciliation durability failure",
                )),
                completed(vec![successor.clone()]),
            ]),
            ..FakeAuthority::default()
        };

        for now_ms in 100..105 {
            worker.poll(&mut authority, now_ms).unwrap();
        }

        assert_eq!(authority.published, vec![blocked.clone(), successor]);
        assert_eq!(authority.poll_count, 5);
        assert!(worker.blocked.contains(&reconciliation_retry_key(&blocked)));
        assert!(worker.in_flight.is_empty());
    }

    #[test]
    fn transfer_failures_quarantine_only_fatal_exact_transition() {
        let fatal = work(7, 11, UnavailablePgReconciliationStage::MetadataTransfer);
        let retryable = work(8, 12, UnavailablePgReconciliationStage::MetadataTransfer);
        let successor = work(9, 13, UnavailablePgReconciliationStage::MetadataTransfer);
        let mut worker = UnavailablePgReconciliationWorker::spawn_with_transfer(
            |work| match work.pg_id().get() {
                7 => Err(ReconciliationTransferError::Fatal(
                    "injected authenticated transfer integrity failure".to_owned(),
                )),
                8 => Err(ReconciliationTransferError::Retryable(
                    "injected transfer timeout".to_owned(),
                )),
                _ => Ok(()),
            },
            Duration::from_secs(60),
        );
        let mut authority = FakeAuthority {
            candidates: VecDeque::from([
                fatal.clone(),
                retryable.clone(),
                successor.clone(),
                fatal.clone(),
                retryable.clone(),
            ]),
            ..FakeAuthority::default()
        };

        for now_ms in 100..10_100 {
            worker.poll(&mut authority, now_ms).unwrap();
            if authority.poll_count == 5 && worker.in_flight.is_empty() {
                break;
            }
            thread::yield_now();
        }

        assert_eq!(authority.poll_count, 5);
        assert_eq!(authority.published, vec![successor]);
        assert!(worker.blocked.contains(&reconciliation_retry_key(&fatal)));
        assert!(worker
            .deferred
            .contains_key(&reconciliation_retry_key(&retryable)));
    }

    #[test]
    fn failed_transfer_members_do_not_discard_successful_batch_peers() {
        let fatal = work(7, 11, UnavailablePgReconciliationStage::MetadataTransfer);
        let retryable = work(8, 11, UnavailablePgReconciliationStage::MetadataTransfer);
        let successful = work(9, 11, UnavailablePgReconciliationStage::MetadataTransfer);
        let mut worker = UnavailablePgReconciliationWorker::spawn_with_transfer(
            |work| match work.pg_id().get() {
                7 => Err(ReconciliationTransferError::Fatal(
                    "injected authenticated transfer integrity failure".to_owned(),
                )),
                8 => Err(ReconciliationTransferError::Retryable(
                    "injected transfer timeout".to_owned(),
                )),
                _ => Ok(()),
            },
            Duration::from_secs(60),
        );
        let mut authority = FakeAuthority {
            candidates: VecDeque::from([fatal.clone(), retryable.clone(), successful.clone()]),
            poll_batch_size: 16,
            ..FakeAuthority::default()
        };

        let deadline = Instant::now() + Duration::from_secs(5);
        while authority.published_batches.is_empty() {
            worker.poll(&mut authority, 100).unwrap();
            assert!(
                Instant::now() < deadline,
                "successful transfer peer did not reach batch activation"
            );
            thread::yield_now();
        }

        assert_eq!(authority.published_batches, vec![vec![successful]]);
        assert!(worker.blocked.contains(&reconciliation_retry_key(&fatal)));
        assert!(worker
            .deferred
            .contains_key(&reconciliation_retry_key(&retryable)));
        assert!(worker.in_flight.is_empty());
        assert!(worker.pending_transfers.is_empty());
        assert!(worker.ready_for_activation.is_empty());
    }

    #[test]
    fn fatal_openraft_completion_is_quarantined() {
        assert!(reconciliation_completion_error_is_fatal(
            &ControlPlaneError::OpenRaftOperation {
                kind: crate::control_plane::ControlPlaneRaftOperationErrorKind::Fatal,
                message: "injected fatal state-machine failure".to_owned(),
            }
        ));
    }

    #[test]
    fn ready_page_is_activated_as_one_batch() {
        let first = work(7, 11, UnavailablePgReconciliationStage::PayloadReadiness);
        let second = work(8, 11, UnavailablePgReconciliationStage::PayloadReadiness);
        let mut worker = UnavailablePgReconciliationWorker::spawn_with_transfer(
            |_| panic!("activation-stage work must not invoke metadata transfer"),
            Duration::ZERO,
        );
        let mut authority = FakeAuthority {
            candidates: VecDeque::from([first.clone(), second.clone()]),
            poll_batch_size: 16,
            ..FakeAuthority::default()
        };

        worker.poll(&mut authority, 100).unwrap();

        assert_eq!(authority.poll_count, 1);
        assert_eq!(authority.published_batches, vec![vec![first, second]]);
        assert!(worker.in_flight.is_empty());
        assert!(worker.pending_transfers.is_empty());
    }

    #[test]
    fn transferred_page_is_accumulated_before_one_activation_batch() {
        let first = work(7, 11, UnavailablePgReconciliationStage::MetadataTransfer);
        let second = work(8, 11, UnavailablePgReconciliationStage::MetadataTransfer);
        let transfer_count = Arc::new(Mutex::new(0));
        let observed_transfer_count = Arc::clone(&transfer_count);
        let mut worker = UnavailablePgReconciliationWorker::spawn_with_transfer(
            move |_| {
                *observed_transfer_count.lock().unwrap() += 1;
                Ok(())
            },
            Duration::ZERO,
        );
        let mut authority = FakeAuthority {
            candidates: VecDeque::from([first.clone(), second.clone()]),
            poll_batch_size: 16,
            ..FakeAuthority::default()
        };
        let metric = |stage| {
            observability::unavailable_pg_worker_stage_metrics_snapshot()
                .into_iter()
                .find(|sample| sample.stage == stage)
                .unwrap()
        };
        let prepare_before = metric(observability::UnavailablePgWorkerStage::Prepare);
        let activation_before = metric(observability::UnavailablePgWorkerStage::Activation);

        let deadline = Instant::now() + Duration::from_secs(5);
        while authority.published_batches.is_empty() {
            worker.poll(&mut authority, 100).unwrap();
            assert!(
                Instant::now() < deadline,
                "transferred page did not reach batch activation"
            );
            thread::yield_now();
        }

        assert_eq!(*transfer_count.lock().unwrap(), 2);
        assert_eq!(authority.published_batches, vec![vec![first, second]]);
        assert!(worker.in_flight.is_empty());
        assert!(worker.pending_transfers.is_empty());
        assert!(worker.ready_for_activation.is_empty());
        let prepare_after = metric(observability::UnavailablePgWorkerStage::Prepare);
        let activation_after = metric(observability::UnavailablePgWorkerStage::Activation);
        assert_eq!(prepare_after.total, prepare_before.total + 2);
        assert_eq!(
            prepare_after.succeeded_total,
            prepare_before.succeeded_total + 2
        );
        assert_eq!(activation_after.total, activation_before.total + 1);
        assert_eq!(
            activation_after.succeeded_total,
            activation_before.succeeded_total + 1
        );
    }

    #[test]
    fn transfer_pool_is_bounded_and_activates_the_complete_page_as_one_batch() {
        let expected = (0..5)
            .map(|offset| {
                work(
                    7 + offset,
                    11,
                    UnavailablePgReconciliationStage::MetadataTransfer,
                )
            })
            .collect::<Vec<_>>();
        let transfer_gate = Arc::new(TransferPoolGate::default());
        let observed_gate = Arc::clone(&transfer_gate);
        let (started_tx, started_rx) = mpsc::sync_channel(expected.len());
        let mut worker = UnavailablePgReconciliationWorker::spawn_with_transfer(
            move |work| {
                let mut state = observed_gate.state.lock().unwrap();
                state.0 += 1;
                state.1 = state.1.max(state.0);
                started_tx.send(work.pg_id()).unwrap();
                while !state.2 {
                    state = observed_gate.changed.wait(state).unwrap();
                }
                state.0 -= 1;
                Ok(())
            },
            Duration::ZERO,
        );
        let _release = TransferPoolGateRelease(Arc::clone(&transfer_gate));
        let mut authority = FakeAuthority {
            candidates: expected.iter().cloned().collect(),
            poll_batch_size: 16,
            ..FakeAuthority::default()
        };

        worker.poll(&mut authority, 100).unwrap();
        let mut started = Vec::new();
        for _ in 0..TRANSFER_WORKER_COUNT {
            started.push(started_rx.recv_timeout(Duration::from_secs(2)).unwrap());
        }
        started.sort_unstable();
        assert_eq!(started.len(), TRANSFER_WORKER_COUNT);
        assert_eq!(worker.in_flight.len(), TRANSFER_WORKER_COUNT);
        assert_eq!(worker.pending_transfers.len(), 1);
        let saturated_queue = observability::unavailable_pg_worker_queue_metrics_snapshot();
        assert_eq!(
            saturated_queue.in_flight_transfer_depth,
            u64::try_from(TRANSFER_WORKER_COUNT).unwrap()
        );
        assert_eq!(saturated_queue.pending_transfer_depth, 1);
        assert!(started_rx.try_recv().is_err());
        {
            let state = transfer_gate.state.lock().unwrap();
            assert_eq!(
                (state.0, state.1),
                (TRANSFER_WORKER_COUNT, TRANSFER_WORKER_COUNT)
            );
        }
        transfer_gate.release();

        let deadline = Instant::now() + Duration::from_secs(5);
        while authority.published_batches.is_empty() {
            worker.poll(&mut authority, 101).unwrap();
            assert!(
                Instant::now() < deadline,
                "bounded transfer page did not reach batch activation"
            );
            thread::yield_now();
        }

        assert_eq!(
            started_rx.recv_timeout(Duration::from_secs(1)).unwrap(),
            expected[4].pg_id()
        );
        assert_eq!(authority.published_batches, vec![expected]);
        assert!(worker.in_flight.is_empty());
        assert!(worker.pending_transfers.is_empty());
        assert!(worker.ready_for_activation.is_empty());
        let drained_queue = observability::unavailable_pg_worker_queue_metrics_snapshot();
        assert_eq!(drained_queue.in_flight_transfer_depth, 0);
        assert_eq!(drained_queue.pending_transfer_depth, 0);
        let state = transfer_gate.state.lock().unwrap();
        assert_eq!((state.0, state.1), (0, TRANSFER_WORKER_COUNT));
    }

    #[test]
    fn mixed_ready_and_transferred_page_is_canonicalized_before_activation() {
        let transferred = work(7, 11, UnavailablePgReconciliationStage::MetadataTransfer);
        let already_ready = work(8, 11, UnavailablePgReconciliationStage::PayloadReadiness);
        let mut worker =
            UnavailablePgReconciliationWorker::spawn_with_transfer(|_| Ok(()), Duration::ZERO);
        let mut authority = FakeAuthority {
            candidates: VecDeque::from([transferred.clone(), already_ready.clone()]),
            poll_batch_size: 16,
            ..FakeAuthority::default()
        };

        let deadline = Instant::now() + Duration::from_secs(5);
        while authority.published_batches.is_empty() {
            worker.poll(&mut authority, 100).unwrap();
            assert!(
                Instant::now() < deadline,
                "mixed-stage page did not reach batch activation"
            );
            thread::yield_now();
        }

        assert_eq!(
            authority.published_batches,
            vec![vec![transferred, already_ready]]
        );
        assert!(worker.in_flight.is_empty());
        assert!(worker.pending_transfers.is_empty());
        assert!(worker.ready_for_activation.is_empty());
    }

    #[test]
    fn fatal_batch_submission_is_not_retried_as_singleton_mutations() {
        let first = work(7, 11, UnavailablePgReconciliationStage::PayloadReadiness);
        let second = work(8, 11, UnavailablePgReconciliationStage::PayloadReadiness);
        let mut worker = UnavailablePgReconciliationWorker::spawn_with_transfer(
            |_| panic!("activation-stage work must not invoke metadata transfer"),
            Duration::ZERO,
        );
        let mut authority = FakeAuthority {
            candidates: VecDeque::from([first.clone(), second.clone()]),
            poll_batch_size: 16,
            publish_results: VecDeque::from([Err(ControlPlaneError::durability_failure(
                "injected batch durability failure",
            ))]),
            ..FakeAuthority::default()
        };

        worker.poll(&mut authority, 100).unwrap();

        assert_eq!(
            authority.published_batches,
            vec![vec![first.clone(), second.clone()]]
        );
        assert!(worker.blocked.contains(&reconciliation_retry_key(&first)));
        assert!(worker.blocked.contains(&reconciliation_retry_key(&second)));
    }

    #[test]
    fn rejected_ready_batch_is_classified_per_member() {
        let stale = work(7, 11, UnavailablePgReconciliationStage::PayloadReadiness);
        let ready = work(8, 11, UnavailablePgReconciliationStage::PayloadReadiness);
        let activation_before = observability::unavailable_pg_worker_stage_metrics_snapshot()
            .into_iter()
            .find(|sample| sample.stage == observability::UnavailablePgWorkerStage::Activation)
            .unwrap();
        let mut worker = UnavailablePgReconciliationWorker::spawn_with_transfer(
            |_| panic!("activation-stage work must not invoke metadata transfer"),
            Duration::from_secs(60),
        );
        let mut authority = FakeAuthority {
            candidates: VecDeque::from([stale.clone(), ready.clone()]),
            poll_batch_size: 16,
            publish_results: VecDeque::from([Ok(UnavailablePgReconciliationCompletionBatch {
                completed: vec![ready.clone()],
                rejected: vec![(
                    stale.clone(),
                    ControlPlaneError::CommandDecode {
                        message: "stale destination lease".to_owned(),
                    },
                )],
                rederive: Vec::new(),
                snapshot: ClusterControlSnapshot::empty(),
            })]),
            ..FakeAuthority::default()
        };

        worker.poll(&mut authority, 100).unwrap();

        assert_eq!(
            authority.published_batches,
            vec![vec![stale.clone(), ready]]
        );
        assert!(worker
            .deferred
            .contains_key(&reconciliation_retry_key(&stale)));
        let activation_after = observability::unavailable_pg_worker_stage_metrics_snapshot()
            .into_iter()
            .find(|sample| sample.stage == observability::UnavailablePgWorkerStage::Activation)
            .unwrap();
        assert_eq!(activation_after.total, activation_before.total + 1);
        assert_eq!(
            activation_after.deferred_total,
            activation_before.deferred_total + 1
        );
        assert_eq!(
            activation_after.succeeded_total,
            activation_before.succeeded_total
        );
        assert_eq!(activation_after.fatal_total, activation_before.fatal_total);
    }

    #[test]
    fn activation_metrics_give_fatal_member_precedence_over_deferred_member() {
        let stale = work(7, 11, UnavailablePgReconciliationStage::PayloadReadiness);
        let corrupt = work(8, 11, UnavailablePgReconciliationStage::PayloadReadiness);
        let ready = work(9, 11, UnavailablePgReconciliationStage::PayloadReadiness);
        let activation_before = observability::unavailable_pg_worker_stage_metrics_snapshot()
            .into_iter()
            .find(|sample| sample.stage == observability::UnavailablePgWorkerStage::Activation)
            .unwrap();
        let mut worker = UnavailablePgReconciliationWorker::spawn_with_transfer(
            |_| panic!("activation-stage work must not invoke metadata transfer"),
            Duration::from_secs(60),
        );
        let mut authority = FakeAuthority {
            candidates: VecDeque::from([stale.clone(), corrupt.clone(), ready.clone()]),
            poll_batch_size: 16,
            publish_results: VecDeque::from([Ok(UnavailablePgReconciliationCompletionBatch {
                completed: vec![ready],
                rejected: vec![
                    (
                        stale.clone(),
                        ControlPlaneError::CommandDecode {
                            message: "stale destination lease".to_owned(),
                        },
                    ),
                    (
                        corrupt.clone(),
                        ControlPlaneError::durability_failure(
                            "injected activation durability failure",
                        ),
                    ),
                ],
                rederive: Vec::new(),
                snapshot: ClusterControlSnapshot::empty(),
            })]),
            ..FakeAuthority::default()
        };

        worker.poll(&mut authority, 100).unwrap();

        assert!(worker
            .deferred
            .contains_key(&reconciliation_retry_key(&stale)));
        assert!(worker.blocked.contains(&reconciliation_retry_key(&corrupt)));
        let activation_after = observability::unavailable_pg_worker_stage_metrics_snapshot()
            .into_iter()
            .find(|sample| sample.stage == observability::UnavailablePgWorkerStage::Activation)
            .unwrap();
        assert_eq!(activation_after.total, activation_before.total + 1);
        assert_eq!(
            activation_after.fatal_total,
            activation_before.fatal_total + 1
        );
        assert_eq!(
            activation_after.succeeded_total,
            activation_before.succeeded_total
        );
        assert_eq!(
            activation_after.deferred_total,
            activation_before.deferred_total
        );
    }

    #[test]
    fn retryable_staging_maintenance_failure_uses_bounded_backoff() {
        for error in [
            ControlPlaneError::AuthorityNotServing,
            ControlPlaneError::StagingEvidencePublicationDeferred,
            ControlPlaneError::StagingEvidencePublicationOutcomeUnconfirmed {
                message: "injected response loss".to_owned(),
            },
        ] {
            let mut worker = UnavailablePgReconciliationWorker::spawn_with_transfer(
                |_| Ok(()),
                Duration::from_secs(60),
            );
            let mut authority = FakeAuthority {
                maintenance_results: VecDeque::from([Err(error)]),
                ..FakeAuthority::default()
            };

            worker.poll(&mut authority, 100).unwrap();
            assert_eq!(authority.maintenance_count, 1);
            assert!(worker.last_maintenance_diagnostic.is_some());

            worker.poll(&mut authority, 101).unwrap();
            assert_eq!(authority.maintenance_count, 1);
            assert!(worker.last_maintenance_diagnostic.is_some());
        }
    }

    #[test]
    fn fatal_staging_maintenance_failure_propagates_without_becoming_retryable() {
        let mut worker =
            UnavailablePgReconciliationWorker::spawn_with_transfer(|_| Ok(()), Duration::ZERO);
        let mut authority = FakeAuthority {
            maintenance_results: VecDeque::from([Err(
                ControlPlaneError::SnapshotInvariantViolation {
                    context: "injected staging maintenance",
                    message: "corrupt checkpoint catalogue".to_owned(),
                },
            )]),
            ..FakeAuthority::default()
        };
        let retry_not_before = worker.maintenance_retry_not_before;

        let error = worker.poll(&mut authority, 100).unwrap_err();
        assert!(matches!(
            error,
            ControlPlaneError::SnapshotInvariantViolation { .. }
        ));
        assert_eq!(authority.maintenance_count, 1);
        assert!(worker.last_maintenance_diagnostic.is_none());
        assert_eq!(worker.maintenance_retry_not_before, retry_not_before);

        for fatal in [
            ControlPlaneError::durability_failure("injected staging durability failure"),
            ControlPlaneError::OpenRaftOperation {
                kind: crate::control_plane::ControlPlaneRaftOperationErrorKind::Fatal,
                message: "injected staging Raft failure".to_owned(),
            },
        ] {
            assert!(!staging_maintenance_error_is_retryable(&fatal));
        }
    }
}
