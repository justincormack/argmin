// Copyright The Argmin Authors.
// SPDX-License-Identifier: Apache-2.0

use std::collections::BTreeMap;
use std::sync::mpsc::{self, Receiver, SyncSender, TryRecvError};
use std::thread;
use std::time::{Duration, Instant};

use crate::control_plane::{
    ControlPlaneError, FileControlPlaneStore, SingleAuthorityControlPlane,
    UnavailablePgReconciliationCursor, UnavailablePgReconciliationStage,
    UnavailablePgReconciliationWork,
};
use crate::{ClusterEpoch, ControlPlaneRaftAuthorityHost, LivePgMetadataTransferAdmin, PgId};

const RETRY_BACKOFF: Duration = Duration::from_secs(1);

struct TransferCompletion {
    work: UnavailablePgReconciliationWork,
    result: Result<(), ReconciliationTransferError>,
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
    completion_rx: Receiver<TransferCompletion>,
    cursor: UnavailablePgReconciliationCursor,
    in_flight: Option<UnavailablePgReconciliationWork>,
    transfer_completed: bool,
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
            + 'static,
        retry_backoff: Duration,
    ) -> Self {
        let (work_tx, work_rx) = mpsc::sync_channel(1);
        let (completion_tx, completion_rx) = mpsc::channel();
        thread::Builder::new()
            .name("unavailable-pg-reconciler".to_owned())
            .spawn(move || {
                while let Ok(work) = work_rx.recv() {
                    let result = transfer(&work);
                    if completion_tx
                        .send(TransferCompletion { work, result })
                        .is_err()
                    {
                        break;
                    }
                }
            })
            .expect("failed to spawn unavailable PG reconciliation worker");
        Self {
            work_tx,
            completion_rx,
            cursor: UnavailablePgReconciliationCursor::start(),
            in_flight: None,
            transfer_completed: false,
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
        if work.stage() == UnavailablePgReconciliationStage::PayloadReadiness {
            self.in_flight = Some(work);
            self.transfer_completed = true;
            self.clear_diagnostic();
            return;
        }
        match self.work_tx.try_send(work.clone()) {
            Ok(()) => {
                self.in_flight = Some(work);
                self.clear_diagnostic();
            }
            Err(error) => {
                self.record_diagnostic(format!("transfer worker is unavailable: {error}"));
                self.authority_retry_not_before = Instant::now() + self.retry_backoff;
            }
        }
    }

    fn defer(&mut self, work: &UnavailablePgReconciliationWork) {
        self.deferred.insert(
            work.pg_id(),
            (work.transition_epoch(), Instant::now() + self.retry_backoff),
        );
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
        let completion = match self.completion_rx.try_recv() {
            Ok(completion) => completion,
            Err(TryRecvError::Empty) => return,
            Err(TryRecvError::Disconnected) => {
                panic!("unavailable PG reconciliation transfer worker terminated unexpectedly")
            }
        };
        if self.in_flight.as_ref() != Some(&completion.work) {
            self.record_diagnostic(
                "transfer worker returned a result for a different transition".to_owned(),
            );
            self.in_flight = None;
            self.transfer_completed = false;
            self.authority_retry_not_before = Instant::now() + self.retry_backoff;
            return;
        }
        match completion.result {
            Ok(()) => {
                self.transfer_completed = true;
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
                        .insert(completion.work.pg_id(), completion.work.transition_epoch());
                } else {
                    self.record_diagnostic(diagnostic);
                    self.defer(&completion.work);
                }
                self.in_flight = None;
                self.transfer_completed = false;
            }
        }
    }

    fn poll(&mut self, authority: &mut impl ReconciliationAuthority, now_ms: u64) {
        self.observe_transfer_worker();
        if Instant::now() < self.authority_retry_not_before {
            return;
        }
        if self.transfer_completed {
            let work = self
                .in_flight
                .as_ref()
                .expect("completed transfer must retain exact work")
                .clone();
            match authority.complete_reconciliation(&work, now_ms) {
                Ok(_) => {
                    self.deferred.remove(&work.pg_id());
                    self.blocked.remove(&work.pg_id());
                    self.in_flight = None;
                    self.transfer_completed = false;
                    self.clear_diagnostic();
                }
                Err(error) => {
                    let diagnostic = error.to_string();
                    if reconciliation_completion_error_is_fatal(&error) {
                        eprintln!(
                            "unavailable PG reconciliation blocked by fatal error: {diagnostic}"
                        );
                        self.blocked.insert(work.pg_id(), work.transition_epoch());
                    } else {
                        self.record_diagnostic(diagnostic);
                        self.defer(&work);
                    }
                    self.in_flight = None;
                    self.transfer_completed = false;
                }
            }
            return;
        }
        if self.in_flight.is_some() {
            return;
        }
        match authority.poll_reconciliation(&mut self.cursor, now_ms) {
            Ok(Some(work)) => {
                if !self.candidate_is_deferred_or_blocked(&work) {
                    self.dispatch(work);
                }
            }
            Ok(None) => {}
            Err(error) => {
                self.record_diagnostic(error.to_string());
                self.authority_retry_not_before = Instant::now() + self.retry_backoff;
            }
        }
    }
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
        poll_count: usize,
        published: Vec<UnavailablePgReconciliationWork>,
        publish_results: VecDeque<Result<bool, ControlPlaneError>>,
    }

    impl ReconciliationAuthority for FakeAuthority {
        fn poll_reconciliation(
            &mut self,
            _cursor: &mut UnavailablePgReconciliationCursor,
            _now_ms: u64,
        ) -> Result<Option<UnavailablePgReconciliationWork>, ControlPlaneError> {
            self.poll_count += 1;
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

        worker.poll(&mut authority, 100);
        assert_eq!(
            started_rx.recv_timeout(Duration::from_secs(1)).unwrap(),
            expected
        );
        for _ in 0..100 {
            worker.poll(&mut authority, 101);
        }
        assert_eq!(authority.poll_count, 1);
        assert!(authority.published.is_empty());

        let (lock, ready) = &*transfer_gate;
        *lock.lock().unwrap() = true;
        ready.notify_all();
        for _ in 0..1_000 {
            worker.poll(&mut authority, 102);
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
        assert_eq!(worker.in_flight, Some(expected));
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
            candidates: VecDeque::from([stale.clone(), successor.clone()]),
            publish_results: VecDeque::from([Ok(false), Ok(true)]),
            ..FakeAuthority::default()
        };

        for now_ms in 100..104 {
            worker.poll(&mut authority, now_ms);
        }

        assert_eq!(authority.published, vec![stale, successor]);
        assert_eq!(*transfer_count.lock().unwrap(), 0);
        assert_eq!(authority.poll_count, 2);
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
        assert_eq!(authority.poll_count, 2);
        assert_eq!(
            worker.deferred.get(&deferred.pg_id()).map(|entry| entry.0),
            Some(deferred.transition_epoch())
        );
        assert!(worker.in_flight.is_none());
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
                Ok(true),
            ]),
            ..FakeAuthority::default()
        };

        for now_ms in 100..105 {
            worker.poll(&mut authority, now_ms);
        }

        assert_eq!(authority.published, vec![blocked.clone(), successor]);
        assert_eq!(authority.poll_count, 3);
        assert_eq!(
            worker.blocked.get(&blocked.pg_id()),
            Some(&blocked.transition_epoch())
        );
        assert!(worker.in_flight.is_none());
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
            if authority.poll_count == 5 && worker.in_flight.is_none() {
                break;
            }
            thread::yield_now();
        }

        assert_eq!(authority.poll_count, 5);
        assert_eq!(authority.published, vec![successor]);
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
    fn fatal_openraft_completion_is_quarantined() {
        assert!(reconciliation_completion_error_is_fatal(
            &ControlPlaneError::OpenRaftOperation {
                kind: crate::control_plane::ControlPlaneRaftOperationErrorKind::Fatal,
                message: "injected fatal state-machine failure".to_owned(),
            }
        ));
    }
}
