// Copyright The Argmin Authors.
// SPDX-License-Identifier: Apache-2.0

use std::collections::{BTreeMap, VecDeque};
use std::panic::{self, AssertUnwindSafe};
use std::sync::mpsc::{self, Receiver, SyncSender, TryRecvError};
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::{Duration, Instant};

use crate::control_plane::{
    ControlPlaneError, FileControlPlaneStore, SingleAuthorityControlPlane,
    UnavailablePgReconciliationCursor, UnavailablePgReconciliationStage,
    UnavailablePgReconciliationWork,
};
use crate::{ClusterEpoch, ControlPlaneRaftAuthorityHost, LivePgMetadataTransferAdmin, PgId};

const RETRY_BACKOFF: Duration = Duration::from_secs(1);
const MAX_CONCURRENT_TRANSFERS: usize = 4;
const MAX_COMPLETION_ATTEMPTS_PER_POLL: usize = MAX_CONCURRENT_TRANSFERS;
const MAX_SCAN_ATTEMPTS_PER_POLL: usize = 16;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum ReconciliationAuthorityErrorDisposition {
    ContinueScan,
    Backoff,
    FailStop,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum ReconciliationCompletionErrorDisposition {
    Retry,
    Quarantine,
    FailStop,
}

enum TransferEvent {
    Completed {
        work: UnavailablePgReconciliationWork,
        result: Result<(), ReconciliationTransferError>,
        elapsed: Duration,
    },
    WorkerPanicked,
}

enum ReconciliationTransferError {
    Retryable(String),
    Fatal(String),
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
    fn poll_reconciliation(
        &mut self,
        cursor: &mut UnavailablePgReconciliationCursor,
        now_ms: u64,
    ) -> Result<Option<UnavailablePgReconciliationWork>, ControlPlaneError>;

    fn complete_reconciliation(
        &mut self,
        work: &UnavailablePgReconciliationWork,
        now_ms: u64,
    ) -> Result<bool, ControlPlaneError>;
}

impl ReconciliationAuthority for ControlPlaneRaftAuthorityHost {
    fn poll_reconciliation(
        &mut self,
        cursor: &mut UnavailablePgReconciliationCursor,
        now_ms: u64,
    ) -> Result<Option<UnavailablePgReconciliationWork>, ControlPlaneError> {
        self.poll_unavailable_pg_reconciliation(cursor, now_ms)
    }

    fn complete_reconciliation(
        &mut self,
        work: &UnavailablePgReconciliationWork,
        now_ms: u64,
    ) -> Result<bool, ControlPlaneError> {
        self.complete_unavailable_pg_reconciliation(work, now_ms)
    }
}

impl ReconciliationAuthority for SingleAuthorityControlPlane<FileControlPlaneStore> {
    fn poll_reconciliation(
        &mut self,
        cursor: &mut UnavailablePgReconciliationCursor,
        now_ms: u64,
    ) -> Result<Option<UnavailablePgReconciliationWork>, ControlPlaneError> {
        self.poll_unavailable_pg_reconciliation(cursor, now_ms)
    }

    fn complete_reconciliation(
        &mut self,
        work: &UnavailablePgReconciliationWork,
        now_ms: u64,
    ) -> Result<bool, ControlPlaneError> {
        self.complete_unavailable_pg_reconciliation(work, now_ms)
    }
}

pub struct UnavailablePgReconciliationWorker {
    work_tx: SyncSender<UnavailablePgReconciliationWork>,
    completion_rx: Receiver<TransferEvent>,
    cursor: UnavailablePgReconciliationCursor,
    in_flight: BTreeMap<PgId, UnavailablePgReconciliationWork>,
    ready_to_complete: VecDeque<UnavailablePgReconciliationWork>,
    completion_retries: BTreeMap<PgId, (UnavailablePgReconciliationWork, Instant)>,
    completion_retry_cursor: Option<PgId>,
    authority_retry_not_before: Instant,
    deferred: BTreeMap<PgId, (ClusterEpoch, Instant)>,
    blocked: BTreeMap<PgId, ClusterEpoch>,
    retry_backoff: Duration,
    last_diagnostic: Option<String>,
}

impl UnavailablePgReconciliationWorker {
    #[must_use]
    pub fn spawn(admin: LivePgMetadataTransferAdmin) -> Self {
        Self::spawn_with_transfer(
            move |work| {
                admin
                    .transfer_unavailable_pg_reconciliation(work)
                    .map(|_| ())
                    .map_err(|error| {
                        let diagnostic = error.to_string();
                        if error.is_fatal() {
                            ReconciliationTransferError::Fatal(diagnostic)
                        } else {
                            ReconciliationTransferError::Retryable(diagnostic)
                        }
                    })
            },
            RETRY_BACKOFF,
        )
    }

    fn spawn_with_transfer(
        transfer: impl Fn(&UnavailablePgReconciliationWork) -> Result<(), ReconciliationTransferError>
            + Send
            + Sync
            + 'static,
        retry_backoff: Duration,
    ) -> Self {
        let (work_tx, work_rx) = mpsc::sync_channel(MAX_CONCURRENT_TRANSFERS);
        let (completion_tx, completion_rx) = mpsc::channel();
        let work_rx = Arc::new(Mutex::new(work_rx));
        let transfer = Arc::new(transfer);
        for worker_index in 0..MAX_CONCURRENT_TRANSFERS {
            let work_rx = Arc::clone(&work_rx);
            let completion_tx = completion_tx.clone();
            let transfer = Arc::clone(&transfer);
            thread::Builder::new()
                .name(format!("unavailable-pg-reconciler-{worker_index}"))
                .spawn(move || loop {
                    let work = {
                        let receiver = work_rx.lock().unwrap_or_else(|error| error.into_inner());
                        match receiver.recv() {
                            Ok(work) => work,
                            Err(_) => break,
                        }
                    };
                    let started_at = Instant::now();
                    let result = panic::catch_unwind(AssertUnwindSafe(|| transfer(&work)));
                    let elapsed = started_at.elapsed();
                    let worker_panicked = result.is_err();
                    let event = match result {
                        Ok(result) => TransferEvent::Completed {
                            work,
                            result,
                            elapsed,
                        },
                        Err(_) => TransferEvent::WorkerPanicked,
                    };
                    if completion_tx.send(event).is_err() || worker_panicked {
                        break;
                    }
                })
                .expect("failed to spawn unavailable PG reconciliation worker");
        }
        drop(completion_tx);
        Self {
            work_tx,
            completion_rx,
            cursor: UnavailablePgReconciliationCursor::start(),
            in_flight: BTreeMap::new(),
            ready_to_complete: VecDeque::new(),
            completion_retries: BTreeMap::new(),
            completion_retry_cursor: None,
            authority_retry_not_before: Instant::now(),
            deferred: BTreeMap::new(),
            blocked: BTreeMap::new(),
            retry_backoff,
            last_diagnostic: None,
        }
    }

    pub fn poll_single_authority(
        &mut self,
        authority: &mut SingleAuthorityControlPlane<FileControlPlaneStore>,
        now_ms: u64,
    ) {
        self.poll(authority, now_ms);
    }

    pub fn poll_raft(&mut self, authority: &mut ControlPlaneRaftAuthorityHost, now_ms: u64) {
        self.poll(authority, now_ms);
    }

    /// Observe transfer completion and fail-stop worker loss independently of authority role.
    pub fn observe_transfer_worker(&mut self) {
        self.receive_completion();
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

    fn dispatch(&mut self, work: UnavailablePgReconciliationWork) {
        let pg_id = work.pg_id();
        if self.in_flight.contains_key(&pg_id) {
            return;
        }
        if work.stage() == UnavailablePgReconciliationStage::PayloadReadiness {
            self.in_flight.insert(pg_id, work.clone());
            observability::record_unavailable_pg_reconciliation_dispatch();
            observability::set_unavailable_pg_reconciliation_in_flight(self.in_flight.len());
            self.ready_to_complete.push_back(work);
            self.clear_diagnostic();
            return;
        }
        match self.work_tx.try_send(work.clone()) {
            Ok(()) => {
                self.in_flight.insert(pg_id, work);
                observability::record_unavailable_pg_reconciliation_dispatch();
                observability::set_unavailable_pg_reconciliation_in_flight(self.in_flight.len());
                self.clear_diagnostic();
            }
            Err(error) => {
                self.record_diagnostic(format!("transfer worker is unavailable: {error}"));
                self.authority_retry_not_before = Instant::now() + self.retry_backoff;
            }
        }
    }

    fn defer(&mut self, work: &UnavailablePgReconciliationWork) {
        observability::record_unavailable_pg_reconciliation_deferred();
        self.deferred.insert(
            work.pg_id(),
            (work.transition_epoch(), Instant::now() + self.retry_backoff),
        );
    }

    fn defer_completion(&mut self, work: UnavailablePgReconciliationWork) {
        debug_assert_eq!(
            work.stage(),
            UnavailablePgReconciliationStage::PayloadReadiness
        );
        self.defer(&work);
        self.completion_retries
            .insert(work.pg_id(), (work, Instant::now() + self.retry_backoff));
    }

    fn promote_due_completion_retries(&mut self, limit: usize) {
        if limit == 0 {
            return;
        }
        let now = Instant::now();
        let mut due = self
            .completion_retries
            .iter()
            .filter_map(|(pg_id, (_, retry_not_before))| {
                (*retry_not_before <= now).then_some(*pg_id)
            })
            .collect::<Vec<_>>();
        if let Some(cursor) = self.completion_retry_cursor {
            let split = due.partition_point(|pg_id| *pg_id <= cursor);
            due.rotate_left(split);
        }
        due.truncate(limit);
        for pg_id in due {
            let Some((work, _)) = self.completion_retries.remove(&pg_id) else {
                continue;
            };
            self.completion_retry_cursor = Some(pg_id);
            if self.in_flight.contains_key(&pg_id) {
                self.completion_retries.insert(pg_id, (work, now));
                continue;
            }
            self.deferred.remove(&pg_id);
            self.in_flight.insert(pg_id, work.clone());
            observability::record_unavailable_pg_reconciliation_dispatch();
            observability::set_unavailable_pg_reconciliation_in_flight(self.in_flight.len());
            self.ready_to_complete.push_back(work);
        }
    }

    fn candidate_is_deferred_or_blocked(&mut self, work: &UnavailablePgReconciliationWork) -> bool {
        let pg_id = work.pg_id();
        let transition_epoch = work.transition_epoch();
        if self
            .blocked
            .get(&pg_id)
            .is_some_and(|blocked_epoch| *blocked_epoch == transition_epoch)
        {
            return true;
        }
        self.blocked.remove(&pg_id);
        if let Some((retained, _)) = self.completion_retries.get(&pg_id) {
            if retained.transition_epoch() == transition_epoch {
                return true;
            }
            self.completion_retries.remove(&pg_id);
        }
        let Some((deferred_epoch, retry_not_before)) = self.deferred.get(&pg_id).copied() else {
            return false;
        };
        if deferred_epoch == transition_epoch && Instant::now() < retry_not_before {
            return true;
        }
        self.deferred.remove(&pg_id);
        false
    }

    fn receive_completion(&mut self) {
        loop {
            let event = match self.completion_rx.try_recv() {
                Ok(event) => event,
                Err(TryRecvError::Empty) => return,
                Err(TryRecvError::Disconnected) => {
                    observability::record_unavailable_pg_reconciliation_fatal();
                    panic!("unavailable PG reconciliation transfer workers terminated unexpectedly")
                }
            };
            let TransferEvent::Completed {
                work,
                result,
                elapsed,
            } = event
            else {
                observability::record_unavailable_pg_reconciliation_fatal();
                panic!("unavailable PG reconciliation transfer worker panicked")
            };
            observability::record_unavailable_pg_reconciliation_transfer(elapsed, result.is_ok());
            let pg_id = work.pg_id();
            assert_eq!(
                self.in_flight.get(&pg_id),
                Some(&work),
                "transfer worker returned a result for a different transition"
            );
            match result {
                Ok(()) => {
                    self.ready_to_complete.push_back(
                        work.with_stage(UnavailablePgReconciliationStage::PayloadReadiness),
                    );
                    self.clear_diagnostic();
                }
                Err(error) => {
                    let fatal = error.is_fatal();
                    let diagnostic = error.diagnostic();
                    if fatal {
                        observability::record_unavailable_pg_reconciliation_fatal();
                        eprintln!(
                            "unavailable PG reconciliation blocked by fatal transfer error: {diagnostic}"
                        );
                        self.blocked.insert(pg_id, work.transition_epoch());
                    } else {
                        self.record_diagnostic(diagnostic);
                        self.defer(&work);
                    }
                    self.in_flight.remove(&pg_id);
                    observability::set_unavailable_pg_reconciliation_in_flight(
                        self.in_flight.len(),
                    );
                }
            }
        }
    }

    fn complete_ready(
        &mut self,
        authority: &mut impl ReconciliationAuthority,
        now_ms: u64,
        remaining_attempts: &mut usize,
    ) {
        while *remaining_attempts > 0 {
            let Some(work) = self.ready_to_complete.pop_front() else {
                break;
            };
            *remaining_attempts -= 1;
            match authority.complete_reconciliation(&work, now_ms) {
                Ok(_) => {
                    observability::record_unavailable_pg_reconciliation_completion();
                    self.deferred.remove(&work.pg_id());
                    self.completion_retries.remove(&work.pg_id());
                    self.blocked.remove(&work.pg_id());
                    self.clear_diagnostic();
                }
                Err(error) => {
                    let diagnostic = error.to_string();
                    match reconciliation_completion_error_disposition(&error) {
                        ReconciliationCompletionErrorDisposition::Retry => {
                            self.record_diagnostic(diagnostic);
                            self.defer_completion(work.clone());
                        }
                        ReconciliationCompletionErrorDisposition::Quarantine => {
                            observability::record_unavailable_pg_reconciliation_fatal();
                            eprintln!(
                                "unavailable PG reconciliation blocked by fatal error: {diagnostic}"
                            );
                            self.blocked.insert(work.pg_id(), work.transition_epoch());
                        }
                        ReconciliationCompletionErrorDisposition::FailStop => {
                            observability::record_unavailable_pg_reconciliation_fatal();
                            panic!("fatal unavailable PG reconciliation completion error: {error}");
                        }
                    }
                }
            }
            self.in_flight.remove(&work.pg_id());
            observability::set_unavailable_pg_reconciliation_in_flight(self.in_flight.len());
        }
    }

    fn poll(&mut self, authority: &mut impl ReconciliationAuthority, now_ms: u64) {
        self.observe_transfer_worker();
        let mut completion_attempts = MAX_COMPLETION_ATTEMPTS_PER_POLL;
        let retry_slots = completion_attempts.saturating_sub(self.ready_to_complete.len());
        self.promote_due_completion_retries(retry_slots);
        self.complete_ready(authority, now_ms, &mut completion_attempts);
        if Instant::now() < self.authority_retry_not_before {
            return;
        }

        let mut scan_attempts = 0;
        while self.in_flight.len() < MAX_CONCURRENT_TRANSFERS
            && scan_attempts < MAX_SCAN_ATTEMPTS_PER_POLL
        {
            scan_attempts += 1;
            let cursor_before = self.cursor;
            match authority.poll_reconciliation(&mut self.cursor, now_ms) {
                Ok(Some(work)) => {
                    if self.in_flight.contains_key(&work.pg_id())
                        || self.candidate_is_deferred_or_blocked(&work)
                    {
                        continue;
                    }
                    self.dispatch(work);
                }
                Ok(None) => break,
                Err(error) => {
                    let cursor_advanced = self.cursor != cursor_before;
                    match reconciliation_authority_error_disposition(&error, cursor_advanced) {
                        ReconciliationAuthorityErrorDisposition::ContinueScan => {
                            self.record_diagnostic(error.to_string());
                            observability::record_unavailable_pg_reconciliation_deferred();
                        }
                        ReconciliationAuthorityErrorDisposition::Backoff => {
                            self.record_diagnostic(error.to_string());
                            observability::record_unavailable_pg_reconciliation_deferred();
                            self.authority_retry_not_before = Instant::now() + self.retry_backoff;
                            break;
                        }
                        ReconciliationAuthorityErrorDisposition::FailStop => {
                            observability::record_unavailable_pg_reconciliation_fatal();
                            panic!("fatal unavailable PG reconciliation authority error: {error}");
                        }
                    }
                }
            }
        }
        self.complete_ready(authority, now_ms, &mut completion_attempts);
    }
}

fn reconciliation_authority_error_disposition(
    error: &ControlPlaneError,
    cursor_advanced: bool,
) -> ReconciliationAuthorityErrorDisposition {
    if cursor_advanced
        && matches!(
            error,
            ControlPlaneError::CommandDecode { .. }
                | ControlPlaneError::NodeLeaseExpired { .. }
                | ControlPlaneError::PgHasNoServingPrimary { .. }
                | ControlPlaneError::PgPrimaryMissingActiveObservation { .. }
                | ControlPlaneError::PgPrimaryObservationNotActive { .. }
                | ControlPlaneError::PgPeeringPendingMetadataCommand { .. }
        )
    {
        return ReconciliationAuthorityErrorDisposition::ContinueScan;
    }
    if error.is_retryable_control_plane_rpc_transport_error()
        || error.is_retryable_authority_clock_wait_error()
        || error.is_retryable_openraft_leadership_error()
        || matches!(
            error,
            ControlPlaneError::AuthorityNotServing
                | ControlPlaneError::AuthorityClockNotLocalServingRaftAuthority
                | ControlPlaneError::ControlPlaneReadIndexNotApplied { .. }
                | ControlPlaneError::RpcUnconfirmed { .. }
        )
    {
        return ReconciliationAuthorityErrorDisposition::Backoff;
    }
    ReconciliationAuthorityErrorDisposition::FailStop
}

fn reconciliation_completion_error_disposition(
    error: &ControlPlaneError,
) -> ReconciliationCompletionErrorDisposition {
    if error.is_retryable_control_plane_rpc_transport_error()
        || error.is_retryable_authority_clock_wait_error()
        || error.is_retryable_openraft_leadership_error()
        || matches!(
            error,
            ControlPlaneError::AuthorityNotServing
                | ControlPlaneError::AuthorityClockNotLocalServingRaftAuthority
                | ControlPlaneError::ControlPlaneReadIndexNotApplied { .. }
                | ControlPlaneError::RpcUnconfirmed { .. }
                | ControlPlaneError::CommandDecode { .. }
                | ControlPlaneError::NodeLeaseExpired { .. }
                | ControlPlaneError::PgHasNoServingPrimary { .. }
                | ControlPlaneError::PgPrimaryMissingActiveObservation { .. }
                | ControlPlaneError::PgPrimaryObservationNotActive { .. }
                | ControlPlaneError::PgPeeringPendingMetadataCommand { .. }
        )
    {
        return ReconciliationCompletionErrorDisposition::Retry;
    }
    if matches!(
        error,
        ControlPlaneError::Io { .. }
            | ControlPlaneError::RpcRemote { .. }
            | ControlPlaneError::Parse { .. }
            | ControlPlaneError::AuthorityClockCheckpoint { .. }
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
    ) {
        return ReconciliationCompletionErrorDisposition::FailStop;
    }
    ReconciliationCompletionErrorDisposition::Quarantine
}

#[cfg(test)]
mod tests {
    use std::collections::VecDeque;
    use std::sync::{Arc, Condvar, Mutex};

    use super::*;
    use crate::control_plane::NodeMembershipState;
    use crate::NodeId;

    #[derive(Default)]
    struct FakeAuthority {
        candidates: VecDeque<UnavailablePgReconciliationWork>,
        poll_errors: VecDeque<(ControlPlaneError, bool)>,
        poll_count: usize,
        published: Vec<UnavailablePgReconciliationWork>,
        publish_results: VecDeque<Result<bool, ControlPlaneError>>,
    }

    impl ReconciliationAuthority for FakeAuthority {
        fn poll_reconciliation(
            &mut self,
            cursor: &mut UnavailablePgReconciliationCursor,
            _now_ms: u64,
        ) -> Result<Option<UnavailablePgReconciliationWork>, ControlPlaneError> {
            self.poll_count += 1;
            if let Some((error, advance_cursor)) = self.poll_errors.pop_front() {
                if advance_cursor {
                    *cursor = UnavailablePgReconciliationCursor::for_test_after(PgId::new(1));
                }
                return Err(error);
            }
            Ok(self.candidates.pop_front())
        }

        fn complete_reconciliation(
            &mut self,
            work: &UnavailablePgReconciliationWork,
            _now_ms: u64,
        ) -> Result<bool, ControlPlaneError> {
            self.published.push(work.clone());
            self.publish_results.pop_front().unwrap_or(Ok(true))
        }
    }

    enum AmbiguousAppendStage {
        Begin,
        Completion,
    }

    struct AmbiguousStandaloneAppendAuthority {
        authority: SingleAuthorityControlPlane<FileControlPlaneStore>,
        store: FileControlPlaneStore,
        stage: AmbiguousAppendStage,
        later_candidate: Option<UnavailablePgReconciliationWork>,
        poll_count: usize,
        completion_count: usize,
    }

    impl AmbiguousStandaloneAppendAuthority {
        fn inject_ambiguous_append(&mut self) -> Result<(), ControlPlaneError> {
            self.store.fail_next_journal_file_sync();
            self.authority
                .set_node_membership(NodeId::new(99), NodeMembershipState::Active)
                .map(|_| ())
        }
    }

    impl ReconciliationAuthority for AmbiguousStandaloneAppendAuthority {
        fn poll_reconciliation(
            &mut self,
            cursor: &mut UnavailablePgReconciliationCursor,
            _now_ms: u64,
        ) -> Result<Option<UnavailablePgReconciliationWork>, ControlPlaneError> {
            self.poll_count += 1;
            if matches!(self.stage, AmbiguousAppendStage::Begin) && self.poll_count == 1 {
                *cursor = UnavailablePgReconciliationCursor::for_test_after(PgId::new(1));
                self.inject_ambiguous_append()?;
                unreachable!("the injected ambiguous append must fail")
            }
            Ok(self.later_candidate.take())
        }

        fn complete_reconciliation(
            &mut self,
            _work: &UnavailablePgReconciliationWork,
            _now_ms: u64,
        ) -> Result<bool, ControlPlaneError> {
            self.completion_count += 1;
            if matches!(self.stage, AmbiguousAppendStage::Completion) {
                self.inject_ambiguous_append()?;
                unreachable!("the injected ambiguous append must fail")
            }
            Ok(true)
        }
    }

    fn ambiguous_standalone_authority(
        stage: AmbiguousAppendStage,
        later_candidate: UnavailablePgReconciliationWork,
    ) -> (test_util::TempDir, AmbiguousStandaloneAppendAuthority) {
        let tmp = test_util::tempdir();
        let store = FileControlPlaneStore::new(tmp.path().join("control-plane.state"));
        let authority = SingleAuthorityControlPlane::open(store.clone()).unwrap();
        (
            tmp,
            AmbiguousStandaloneAppendAuthority {
                authority,
                store,
                stage,
                later_candidate: Some(later_candidate),
                poll_count: 0,
                completion_count: 0,
            },
        )
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

    #[test]
    fn one_in_flight_transfer_suppresses_rediscovery_and_publishes_exact_work() {
        let expected = work(7, 11, UnavailablePgReconciliationStage::MetadataTransfer);
        let transfer_gate = Arc::new((Mutex::new(false), Condvar::new()));
        let (started_tx, started_rx) = mpsc::sync_channel(1);
        let (finished_tx, finished_rx) = mpsc::sync_channel(1);
        let worker_gate = Arc::clone(&transfer_gate);
        let mut worker = UnavailablePgReconciliationWorker::spawn_with_transfer(
            move |work| {
                started_tx.send(work.clone()).unwrap();
                let (lock, ready) = &*worker_gate;
                let mut released = lock.lock().unwrap();
                while !*released {
                    released = ready.wait(released).unwrap();
                }
                finished_tx.send(()).unwrap();
                Ok(())
            },
            Duration::ZERO,
        );
        let mut authority = FakeAuthority {
            candidates: VecDeque::from([expected.clone()]),
            ..FakeAuthority::default()
        };

        worker.poll(&mut authority, 100);
        assert_eq!(
            started_rx.recv_timeout(Duration::from_secs(1)).unwrap(),
            expected
        );
        for _ in 0..100 {
            worker.poll(&mut authority, 101);
        }
        assert!(authority.poll_count >= 2);
        assert!(authority.published.is_empty());

        let (lock, ready) = &*transfer_gate;
        *lock.lock().unwrap() = true;
        ready.notify_all();
        finished_rx
            .recv_timeout(Duration::from_secs(1))
            .expect("released transfer worker did not finish its transfer body");
        let publication_deadline = Instant::now() + Duration::from_secs(1);
        while authority.published.is_empty() {
            worker.poll(&mut authority, 102);
            if !authority.published.is_empty() {
                break;
            }
            if Instant::now() >= publication_deadline {
                panic!("completed transfer was not published before the deadline");
            }
            thread::yield_now();
        }
        assert_eq!(
            authority.published,
            vec![expected.with_stage(UnavailablePgReconciliationStage::PayloadReadiness)]
        );
        assert!(authority.poll_count >= 2);
    }

    #[test]
    fn transfer_pool_runs_four_pgs_concurrently_and_refills_after_completion() {
        let expected = (1..=5)
            .map(|pg_id| {
                work(
                    pg_id,
                    10 + u64::from(pg_id),
                    UnavailablePgReconciliationStage::MetadataTransfer,
                )
            })
            .collect::<Vec<_>>();
        let transfer_gate = Arc::new((Mutex::new(false), Condvar::new()));
        let (started_tx, started_rx) = mpsc::sync_channel(MAX_CONCURRENT_TRANSFERS + 1);
        let worker_gate = Arc::clone(&transfer_gate);
        let mut worker = UnavailablePgReconciliationWorker::spawn_with_transfer(
            move |work| {
                started_tx.send(work.pg_id()).unwrap();
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
            candidates: expected.clone().into(),
            ..FakeAuthority::default()
        };

        worker.poll(&mut authority, 100);
        let mut initially_started = Vec::new();
        for _ in 0..MAX_CONCURRENT_TRANSFERS {
            initially_started.push(
                started_rx
                    .recv_timeout(Duration::from_secs(1))
                    .expect("four transfers must start without serial waiting"),
            );
        }
        assert!(started_rx.recv_timeout(Duration::from_millis(50)).is_err());
        assert_eq!(worker.in_flight.len(), MAX_CONCURRENT_TRANSFERS);

        let (lock, ready) = &*transfer_gate;
        *lock.lock().unwrap() = true;
        ready.notify_all();

        let deadline = Instant::now() + Duration::from_secs(2);
        while authority.published.len() != expected.len() {
            worker.poll(&mut authority, 101);
            assert!(
                Instant::now() < deadline,
                "transfer pool did not refill and publish every PG"
            );
            thread::yield_now();
        }
        let fifth = started_rx
            .recv_timeout(Duration::from_secs(1))
            .expect("the fifth transfer must start after a slot is released");
        initially_started.push(fifth);
        initially_started.sort_unstable();
        let mut expected_pg_ids = expected.iter().map(|work| work.pg_id()).collect::<Vec<_>>();
        expected_pg_ids.sort_unstable();
        assert_eq!(initially_started, expected_pg_ids);
        assert!(worker.in_flight.is_empty());
    }

    #[test]
    fn transfer_thread_panic_fails_stop_after_dequeue() {
        let expected = work(7, 11, UnavailablePgReconciliationStage::MetadataTransfer);
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

        worker.poll(&mut authority, 100);
        assert_eq!(
            started_rx.recv_timeout(Duration::from_secs(1)).unwrap(),
            expected
        );

        let deadline = Instant::now() + Duration::from_secs(1);
        loop {
            let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                worker.observe_transfer_worker();
            }));
            if let Err(payload) = result {
                let message = payload
                    .downcast_ref::<&str>()
                    .copied()
                    .or_else(|| payload.downcast_ref::<String>().map(String::as_str))
                    .unwrap_or("non-string panic");
                assert!(
                    message.contains("transfer worker panicked"),
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

        assert!(authority.poll_count >= 1);
        assert_eq!(worker.in_flight.get(&expected.pg_id()), Some(&expected));
        assert!(authority.published.is_empty());
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
            candidates: VecDeque::from([stale.clone()]),
            publish_results: VecDeque::from([Ok(false), Ok(true)]),
            ..FakeAuthority::default()
        };

        worker.poll(&mut authority, 100);
        authority.candidates.push_back(successor.clone());
        worker.poll(&mut authority, 101);

        assert_eq!(authority.published, vec![stale, successor]);
        assert_eq!(*transfer_count.lock().unwrap(), 0);
        assert!(authority.poll_count >= 2);
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
                Ok(true),
            ]),
            ..FakeAuthority::default()
        };

        for now_ms in 100..104 {
            worker.poll(&mut authority, now_ms);
        }

        assert_eq!(authority.published, vec![deferred.clone(), successor]);
        assert!(authority.poll_count >= 2);
        assert_eq!(
            worker.deferred.get(&deferred.pg_id()).map(|entry| entry.0),
            Some(deferred.transition_epoch())
        );
        assert_eq!(
            worker
                .completion_retries
                .get(&deferred.pg_id())
                .map(|entry| &entry.0),
            Some(&deferred)
        );
        assert!(worker.in_flight.is_empty());
    }

    #[test]
    fn retryable_activation_lag_does_not_repeat_metadata_transfer() {
        let transfer = work(7, 11, UnavailablePgReconciliationStage::MetadataTransfer);
        let readiness = transfer
            .clone()
            .with_stage(UnavailablePgReconciliationStage::PayloadReadiness);
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
            candidates: VecDeque::from([transfer]),
            publish_results: VecDeque::from([
                Err(ControlPlaneError::CommandDecode {
                    message: "destination heartbeat has not published readiness".to_owned(),
                }),
                Ok(true),
            ]),
            ..FakeAuthority::default()
        };

        let deadline = Instant::now() + Duration::from_secs(2);
        while authority.published.len() < 2 {
            worker.poll(&mut authority, 100);
            assert!(
                Instant::now() < deadline,
                "activation retry did not retain the completed transfer owner"
            );
            thread::yield_now();
        }

        assert_eq!(*transfer_count.lock().unwrap(), 1);
        assert_eq!(authority.published, vec![readiness.clone(), readiness]);
        assert!(worker.completion_retries.is_empty());
        assert!(worker.in_flight.is_empty());
    }

    #[test]
    fn retryable_poll_race_does_not_block_a_later_pg() {
        let successor = work(8, 12, UnavailablePgReconciliationStage::PayloadReadiness);
        let mut worker = UnavailablePgReconciliationWorker::spawn_with_transfer(
            |_| panic!("activation-stage work must not invoke metadata transfer"),
            Duration::from_secs(60),
        );
        let mut authority = FakeAuthority {
            candidates: VecDeque::from([successor.clone()]),
            poll_errors: VecDeque::from([(
                ControlPlaneError::CommandDecode {
                    message: "candidate lease changed during begin validation".to_owned(),
                },
                true,
            )]),
            ..FakeAuthority::default()
        };

        worker.poll(&mut authority, 100);

        assert_eq!(authority.poll_count, 3);
        assert_eq!(
            authority.published,
            vec![successor.with_stage(UnavailablePgReconciliationStage::PayloadReadiness)]
        );
        assert!(worker.in_flight.is_empty());
    }

    #[test]
    fn authority_wide_post_scan_failures_back_off_after_cursor_advancement() {
        let failures = [
            ControlPlaneError::AuthorityNotServing,
            ControlPlaneError::OpenRaftOperation {
                kind: crate::control_plane::ControlPlaneRaftOperationErrorKind::QuorumNotEnough,
                message: "injected quorum loss after candidate selection".to_owned(),
            },
            ControlPlaneError::io(
                "submit unavailable PG begin",
                std::io::Error::new(std::io::ErrorKind::TimedOut, "injected timeout"),
            ),
        ];
        for error in failures {
            let successor = work(8, 12, UnavailablePgReconciliationStage::PayloadReadiness);
            let mut worker = UnavailablePgReconciliationWorker::spawn_with_transfer(
                |_| panic!("authority-wide failure must not dispatch transfer work"),
                Duration::from_secs(60),
            );
            let mut authority = FakeAuthority {
                candidates: VecDeque::from([successor]),
                poll_errors: VecDeque::from([(error, true)]),
                ..FakeAuthority::default()
            };

            worker.poll(&mut authority, 100);

            assert_eq!(authority.poll_count, 1);
            assert!(authority.published.is_empty());
            assert!(worker.authority_retry_not_before > Instant::now());
        }
    }

    #[test]
    fn retryable_pre_scan_failure_backs_off_without_synchronous_retries() {
        let successor = work(8, 12, UnavailablePgReconciliationStage::PayloadReadiness);
        let mut worker = UnavailablePgReconciliationWorker::spawn_with_transfer(
            |_| panic!("activation-stage work must not invoke metadata transfer"),
            Duration::from_secs(60),
        );
        let mut authority = FakeAuthority {
            candidates: VecDeque::from([successor]),
            poll_errors: VecDeque::from([(
                ControlPlaneError::AuthorityClockLeadershipChanged {
                    established_term: Some(7),
                    current_term: 8,
                },
                false,
            )]),
            ..FakeAuthority::default()
        };

        worker.poll(&mut authority, 100);

        assert_eq!(authority.poll_count, 1);
        assert!(authority.published.is_empty());
        assert!(worker.authority_retry_not_before > Instant::now());
    }

    #[test]
    fn authority_backoff_does_not_delay_an_already_completed_transfer() {
        let completed = work(7, 11, UnavailablePgReconciliationStage::PayloadReadiness);
        let mut worker = UnavailablePgReconciliationWorker::spawn_with_transfer(
            |_| panic!("the completed work is injected directly"),
            Duration::from_secs(60),
        );
        worker
            .in_flight
            .insert(completed.pg_id(), completed.clone());
        worker.ready_to_complete.push_back(completed.clone());
        worker.authority_retry_not_before = Instant::now() + Duration::from_secs(60);
        let mut authority = FakeAuthority::default();

        worker.poll(&mut authority, 100);

        assert_eq!(authority.published, vec![completed]);
        assert_eq!(authority.poll_count, 0);
        assert!(worker.in_flight.is_empty());
    }

    #[test]
    fn fatal_poll_failure_fails_stop_reconciliation() {
        let mut worker = UnavailablePgReconciliationWorker::spawn_with_transfer(
            |_| panic!("fatal polling must not dispatch transfer work"),
            Duration::ZERO,
        );
        let mut authority = FakeAuthority {
            poll_errors: VecDeque::from([(
                ControlPlaneError::durability_failure("injected begin durability failure"),
                true,
            )]),
            ..FakeAuthority::default()
        };

        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            worker.poll(&mut authority, 100);
        }));

        let payload = result.expect_err("fatal polling failure must fail-stop the worker");
        let message = payload
            .downcast_ref::<&str>()
            .copied()
            .or_else(|| payload.downcast_ref::<String>().map(String::as_str))
            .unwrap_or("non-string panic");
        assert!(message.contains("fatal unavailable PG reconciliation authority error"));
        assert_eq!(authority.poll_count, 1);
    }

    #[test]
    fn ambiguous_standalone_begin_append_fail_stops_before_later_candidate() {
        let later = work(8, 12, UnavailablePgReconciliationStage::MetadataTransfer);
        let (_tmp, mut authority) =
            ambiguous_standalone_authority(AmbiguousAppendStage::Begin, later.clone());
        let mut worker = UnavailablePgReconciliationWorker::spawn_with_transfer(
            |_| panic!("durability uncertainty must not dispatch transfer work"),
            Duration::ZERO,
        );

        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            worker.poll(&mut authority, 100);
        }));

        let payload = result.expect_err("ambiguous begin append must fail-stop the worker");
        let message = payload
            .downcast_ref::<&str>()
            .copied()
            .or_else(|| payload.downcast_ref::<String>().map(String::as_str))
            .unwrap_or("non-string panic");
        assert!(message.contains("fatal unavailable PG reconciliation authority error"));
        assert_eq!(authority.poll_count, 1);
        assert_eq!(authority.later_candidate, Some(later));
        assert!(worker.in_flight.is_empty());
    }

    #[test]
    fn ambiguous_standalone_completion_append_fail_stops_before_later_candidate() {
        let completed = work(7, 11, UnavailablePgReconciliationStage::PayloadReadiness);
        let later = work(8, 12, UnavailablePgReconciliationStage::MetadataTransfer);
        let (_tmp, mut authority) =
            ambiguous_standalone_authority(AmbiguousAppendStage::Completion, later.clone());
        let mut worker = UnavailablePgReconciliationWorker::spawn_with_transfer(
            |_| panic!("the completed work is injected directly"),
            Duration::ZERO,
        );
        worker
            .in_flight
            .insert(completed.pg_id(), completed.clone());
        worker.ready_to_complete.push_back(completed);

        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            worker.poll(&mut authority, 100);
        }));

        let payload = result.expect_err("ambiguous completion append must fail-stop the worker");
        let message = payload
            .downcast_ref::<&str>()
            .copied()
            .or_else(|| payload.downcast_ref::<String>().map(String::as_str))
            .unwrap_or("non-string panic");
        assert!(message.contains("fatal unavailable PG reconciliation completion error"));
        assert_eq!(authority.completion_count, 1);
        assert_eq!(authority.poll_count, 0);
        assert_eq!(authority.later_candidate, Some(later));
        assert!(worker.completion_retries.is_empty());
    }

    #[test]
    fn nontransient_io_and_remote_state_failures_fail_stop_polling() {
        let failures = [
            (
                ControlPlaneError::io(
                    "append standalone reconciliation begin",
                    std::io::Error::new(std::io::ErrorKind::PermissionDenied, "injected denial"),
                ),
                true,
            ),
            (
                ControlPlaneError::rpc_remote("injected local Raft state-machine read failure"),
                false,
            ),
        ];
        for (error, cursor_advanced) in failures {
            let mut worker = UnavailablePgReconciliationWorker::spawn_with_transfer(
                |_| panic!("fatal polling must not dispatch transfer work"),
                Duration::ZERO,
            );
            let mut authority = FakeAuthority {
                poll_errors: VecDeque::from([(error, cursor_advanced)]),
                ..FakeAuthority::default()
            };

            let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                worker.poll(&mut authority, 100);
            }));

            let payload = result.expect_err("production fatal polling error must fail-stop");
            let message = payload
                .downcast_ref::<&str>()
                .copied()
                .or_else(|| payload.downcast_ref::<String>().map(String::as_str))
                .unwrap_or("non-string panic");
            assert!(message.contains("fatal unavailable PG reconciliation authority error"));
            assert_eq!(authority.poll_count, 1);
        }
    }

    #[test]
    fn definitive_completion_failure_blocks_only_its_exact_transition() {
        let blocked = work(7, 11, UnavailablePgReconciliationStage::PayloadReadiness);
        let successor = work(8, 12, UnavailablePgReconciliationStage::PayloadReadiness);
        let mut worker = UnavailablePgReconciliationWorker::spawn_with_transfer(
            |_| panic!("activation-stage work must not invoke metadata transfer"),
            Duration::ZERO,
        );
        let mut authority = FakeAuthority {
            candidates: VecDeque::from([blocked.clone(), successor.clone(), blocked.clone()]),
            publish_results: VecDeque::from([
                Err(ControlPlaneError::UnknownPg {
                    pg_id: blocked.pg_id().get(),
                }),
                Ok(true),
            ]),
            ..FakeAuthority::default()
        };

        for now_ms in 100..105 {
            worker.poll(&mut authority, now_ms);
        }

        assert_eq!(authority.published, vec![blocked.clone(), successor]);
        assert!(authority.poll_count >= 3);
        assert_eq!(
            worker.blocked.get(&blocked.pg_id()),
            Some(&blocked.transition_epoch())
        );
        assert!(worker.in_flight.is_empty());
    }

    #[test]
    fn nontransient_completion_authority_failures_fail_stop() {
        let failures = [
            ControlPlaneError::io(
                "checkpoint reconciliation completion",
                std::io::Error::new(std::io::ErrorKind::PermissionDenied, "injected denial"),
            ),
            ControlPlaneError::rpc_remote("injected Raft state-machine read failure"),
        ];
        for error in failures {
            let completion = work(7, 11, UnavailablePgReconciliationStage::PayloadReadiness);
            let mut worker = UnavailablePgReconciliationWorker::spawn_with_transfer(
                |_| panic!("activation-stage work must not invoke metadata transfer"),
                Duration::ZERO,
            );
            let mut authority = FakeAuthority {
                candidates: VecDeque::from([completion]),
                publish_results: VecDeque::from([Err(error)]),
                ..FakeAuthority::default()
            };

            let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                worker.poll(&mut authority, 100);
            }));

            let payload = result.expect_err("nontransient completion failure must fail-stop");
            let message = payload
                .downcast_ref::<&str>()
                .copied()
                .or_else(|| payload.downcast_ref::<String>().map(String::as_str))
                .unwrap_or("non-string panic");
            assert!(message.contains("fatal unavailable PG reconciliation completion error"));
            assert!(worker.completion_retries.is_empty());
        }
    }

    #[test]
    fn completion_retries_are_bounded_and_rotate_past_low_pg_ids() {
        let mut worker = UnavailablePgReconciliationWorker::spawn_with_transfer(
            |_| panic!("activation-stage work must not invoke metadata transfer"),
            Duration::ZERO,
        );
        for pg_id in 1..=6 {
            worker.defer_completion(work(
                pg_id,
                10 + u64::from(pg_id),
                UnavailablePgReconciliationStage::PayloadReadiness,
            ));
        }
        let retry = || {
            Err(ControlPlaneError::CommandDecode {
                message: "destination readiness has not converged".to_owned(),
            })
        };
        let mut authority = FakeAuthority {
            publish_results: std::iter::repeat_with(retry).take(8).collect(),
            ..FakeAuthority::default()
        };

        worker.poll(&mut authority, 100);
        assert_eq!(
            authority
                .published
                .iter()
                .map(|work| work.pg_id().get())
                .collect::<Vec<_>>(),
            vec![1, 2, 3, 4]
        );

        worker.poll(&mut authority, 101);
        assert_eq!(
            authority
                .published
                .iter()
                .map(|work| work.pg_id().get())
                .collect::<Vec<_>>(),
            vec![1, 2, 3, 4, 5, 6, 1, 2]
        );
        assert_eq!(worker.completion_retries.len(), 6);
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
            worker.poll(&mut authority, now_ms);
            if authority.poll_count >= 5 && worker.in_flight.is_empty() {
                break;
            }
            thread::yield_now();
        }

        assert!(authority.poll_count >= 5);
        assert_eq!(
            authority.published,
            vec![successor.with_stage(UnavailablePgReconciliationStage::PayloadReadiness)]
        );
        assert_eq!(
            worker.blocked.get(&fatal.pg_id()),
            Some(&fatal.transition_epoch())
        );
        assert_eq!(
            worker.deferred.get(&retryable.pg_id()).map(|entry| entry.0),
            Some(retryable.transition_epoch())
        );
    }

    #[test]
    fn fatal_openraft_completion_fails_stop() {
        assert_eq!(
            reconciliation_completion_error_disposition(&ControlPlaneError::OpenRaftOperation {
                kind: crate::control_plane::ControlPlaneRaftOperationErrorKind::Fatal,
                message: "injected fatal state-machine failure".to_owned(),
            }),
            ReconciliationCompletionErrorDisposition::FailStop
        );
    }
}
