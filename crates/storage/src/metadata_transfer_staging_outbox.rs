// Copyright The Argmin Authors.
// SPDX-License-Identifier: Apache-2.0

use std::io;
use std::panic::{catch_unwind, AssertUnwindSafe};
use std::sync::{Arc, Condvar, Mutex};
use std::thread::{self, JoinHandle};
use std::time::Duration;

use crate::control_plane::ControlPlaneError;
use crate::control_plane_service_client::ControlPlaneStorageNodeClient;
use crate::pg_store::{
    MetadataTransferStagingEvidenceApplyReceipt, MetadataTransferStagingEvidencePage,
    MetadataTransferStagingStore,
};

const OUTBOX_IDLE_POLL_INTERVAL: Duration = Duration::from_millis(250);
const OUTBOX_SUCCESS_INTERVAL: Duration = Duration::from_millis(25);
const OUTBOX_RETRY_INTERVAL: Duration = Duration::from_secs(1);

pub struct StorageNodeMetadataTransferStagingOutbox {
    stop: Arc<(Mutex<bool>, Condvar)>,
    status: Arc<Mutex<StorageNodeMetadataTransferStagingOutboxStatus>>,
    handle: Option<JoinHandle<()>>,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct StorageNodeMetadataTransferStagingOutboxStatus {
    pub publication_attempts: u64,
    pub publication_successes: u64,
    pub publication_failures: u64,
    pub last_published_generation: Option<u64>,
    pub last_error: Option<String>,
    pub failed: bool,
}

pub(crate) trait MetadataTransferStagingEvidencePublisher {
    fn publish_staging_evidence_page(
        &mut self,
        page: &MetadataTransferStagingEvidencePage,
        authority_now_ms: u64,
    ) -> Result<MetadataTransferStagingEvidenceApplyReceipt, ControlPlaneError>;
}

impl MetadataTransferStagingEvidencePublisher for ControlPlaneStorageNodeClient {
    fn publish_staging_evidence_page(
        &mut self,
        page: &MetadataTransferStagingEvidencePage,
        authority_now_ms: u64,
    ) -> Result<MetadataTransferStagingEvidenceApplyReceipt, ControlPlaneError> {
        self.publish_metadata_transfer_staging_evidence_page(page, authority_now_ms)
    }
}

enum OutboxStep {
    Idle,
    Published { generation: u64 },
    Deferred { error: String },
    Fatal { error: String },
}

impl StorageNodeMetadataTransferStagingOutbox {
    pub(crate) fn spawn<P, F, E>(
        store: Arc<MetadataTransferStagingStore>,
        mut publisher: P,
        authority_now_ms: F,
        on_fatal: E,
    ) -> Result<Self, io::Error>
    where
        P: MetadataTransferStagingEvidencePublisher + Send + 'static,
        F: Fn() -> u64 + Send + 'static,
        E: Fn(String) + Send + 'static,
    {
        let stop = Arc::new((Mutex::new(false), Condvar::new()));
        let status = Arc::new(Mutex::new(
            StorageNodeMetadataTransferStagingOutboxStatus::default(),
        ));
        let worker_stop = Arc::clone(&stop);
        let worker_status = Arc::clone(&status);
        let handle = thread::Builder::new()
            .name("argmin-storage-staging-evidence-outbox".to_owned())
            .spawn(move || {
                let result = catch_unwind(AssertUnwindSafe(|| loop {
                    let step = run_outbox_step(&store, &mut publisher, authority_now_ms());
                    let (delay, fatal) = record_outbox_step(&worker_status, step);
                    if fatal.is_some() || wait_for_outbox(&worker_stop, delay) {
                        return fatal;
                    }
                }));
                let fatal = match result {
                    Ok(fatal) => fatal,
                    Err(_) => {
                        let error = "metadata-transfer staging evidence outbox panicked".to_owned();
                        let mut status = worker_status
                            .lock()
                            .unwrap_or_else(|poisoned| poisoned.into_inner());
                        status.publication_failures = status.publication_failures.saturating_add(1);
                        status.last_error = Some(error.clone());
                        status.failed = true;
                        Some(error)
                    }
                };
                if let Some(error) = fatal {
                    on_fatal(error);
                }
            })?;
        Ok(Self {
            stop,
            status,
            handle: Some(handle),
        })
    }

    #[must_use]
    pub fn status(&self) -> StorageNodeMetadataTransferStagingOutboxStatus {
        self.status
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .clone()
    }

    pub fn stop(&mut self) {
        {
            let (lock, condition) = &*self.stop;
            let mut stopped = lock.lock().unwrap_or_else(|poisoned| poisoned.into_inner());
            *stopped = true;
            condition.notify_all();
        }
        if let Some(handle) = self.handle.take() {
            let _ = handle.join();
        }
    }
}

fn record_outbox_step(
    status: &Mutex<StorageNodeMetadataTransferStagingOutboxStatus>,
    step: OutboxStep,
) -> (Duration, Option<String>) {
    let mut status = status
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner());
    match step {
        OutboxStep::Idle => (OUTBOX_IDLE_POLL_INTERVAL, None),
        OutboxStep::Published { generation } => {
            status.publication_attempts = status.publication_attempts.saturating_add(1);
            status.publication_successes = status.publication_successes.saturating_add(1);
            status.last_published_generation = Some(generation);
            status.last_error = None;
            (OUTBOX_SUCCESS_INTERVAL, None)
        }
        OutboxStep::Deferred { error } => {
            if status.last_error.as_deref() != Some(error.as_str()) {
                eprintln!("storage-node staging evidence publication deferred: {error}");
            }
            status.publication_attempts = status.publication_attempts.saturating_add(1);
            status.publication_failures = status.publication_failures.saturating_add(1);
            status.last_error = Some(error);
            (OUTBOX_RETRY_INTERVAL, None)
        }
        OutboxStep::Fatal { error } => {
            status.publication_attempts = status.publication_attempts.saturating_add(1);
            status.publication_failures = status.publication_failures.saturating_add(1);
            status.last_error = Some(error.clone());
            status.failed = true;
            (Duration::ZERO, Some(error))
        }
    }
}

impl Drop for StorageNodeMetadataTransferStagingOutbox {
    fn drop(&mut self) {
        self.stop();
    }
}

fn run_outbox_step(
    store: &MetadataTransferStagingStore,
    publisher: &mut impl MetadataTransferStagingEvidencePublisher,
    authority_now_ms: u64,
) -> OutboxStep {
    let page = match store.next_evidence_page() {
        Ok(Some(page)) => page,
        Ok(None) => return OutboxStep::Idle,
        Err(error) => {
            return OutboxStep::Fatal {
                error: format!("load durable staging evidence page: {error}"),
            };
        }
    };
    let receipt = match publisher.publish_staging_evidence_page(&page, authority_now_ms) {
        Ok(receipt) => receipt,
        Err(error) if error.is_retryable_staging_evidence_publication_error() => {
            return OutboxStep::Deferred {
                error: error.to_string(),
            };
        }
        Err(error) => {
            return OutboxStep::Fatal {
                error: format!("publish staging evidence page: {error}"),
            };
        }
    };
    if let Err(error) = store.record_evidence_apply_receipt(&page, &receipt) {
        return OutboxStep::Fatal {
            error: format!("record staging evidence apply receipt: {error}"),
        };
    }
    OutboxStep::Published {
        generation: page.generation(),
    }
}

fn wait_for_outbox(stop: &Arc<(Mutex<bool>, Condvar)>, delay: Duration) -> bool {
    let (lock, condition) = &**stop;
    let stopped = lock.lock().unwrap_or_else(|poisoned| poisoned.into_inner());
    if *stopped {
        return true;
    }
    let (stopped, _) = condition
        .wait_timeout(stopped, delay)
        .unwrap_or_else(|poisoned| poisoned.into_inner());
    *stopped
}

#[cfg(test)]
mod tests {
    use std::collections::VecDeque;
    use std::sync::mpsc;

    use placement::NodeId;

    use super::*;
    use crate::control_plane::UnavailablePgTransitionMutationBinding;
    use crate::pg_store::{
        MetadataTransferStagingIntent, MetadataTransferStagingLimits,
        MetadataTransferStagingNodeIdentity, METADATA_TRANSFER_STAGED_ARTIFACT_FORMAT_VERSION,
    };
    use crate::{ClusterEpoch, PgId};

    struct RecordingPublisher {
        outcomes: VecDeque<Result<(), ControlPlaneError>>,
        pages: Vec<MetadataTransferStagingEvidencePage>,
    }

    struct PanickingPublisher;

    impl MetadataTransferStagingEvidencePublisher for PanickingPublisher {
        fn publish_staging_evidence_page(
            &mut self,
            _page: &MetadataTransferStagingEvidencePage,
            _authority_now_ms: u64,
        ) -> Result<MetadataTransferStagingEvidenceApplyReceipt, ControlPlaneError> {
            panic!("injected staging evidence publisher panic")
        }
    }

    impl MetadataTransferStagingEvidencePublisher for RecordingPublisher {
        fn publish_staging_evidence_page(
            &mut self,
            page: &MetadataTransferStagingEvidencePage,
            _authority_now_ms: u64,
        ) -> Result<MetadataTransferStagingEvidenceApplyReceipt, ControlPlaneError> {
            self.pages.push(page.clone());
            self.outcomes
                .pop_front()
                .unwrap_or(Ok(()))
                .map(|()| MetadataTransferStagingEvidenceApplyReceipt::for_page(page))
        }
    }

    fn store_with_publication() -> (test_util::TempDir, MetadataTransferStagingStore) {
        let root = test_util::tempdir();
        let identity = MetadataTransferStagingNodeIdentity::new(
            NodeId::new(4),
            9,
            "unix:///run/argmin/storage-4.sock".to_owned(),
        )
        .unwrap();
        let store = MetadataTransferStagingStore::open(
            root.path(),
            identity,
            MetadataTransferStagingLimits::new(8, 1024 * 1024, 4 * 1024 * 1024).unwrap(),
        )
        .unwrap();
        let artifact = b"durable staging evidence outbox artifact";
        let binding = UnavailablePgTransitionMutationBinding::new(
            PgId::new(19),
            ClusterEpoch::new(12).unwrap(),
            ClusterEpoch::new(11).unwrap(),
            vec![NodeId::new(1), NodeId::new(2), NodeId::new(3)],
            vec![NodeId::new(4), NodeId::new(2), NodeId::new(3)],
        );
        let intent = MetadataTransferStagingIntent::for_unavailable_transition(
            &binding,
            checksum::sha256::digest(artifact),
            u64::try_from(artifact.len()).unwrap(),
            METADATA_TRANSFER_STAGED_ARTIFACT_FORMAT_VERSION,
        )
        .unwrap();
        store.create_intent(&intent).unwrap();
        store.publish_artifact(&intent, artifact).unwrap();
        (root, store)
    }

    #[test]
    fn response_loss_replays_the_exact_page_before_acknowledgement() {
        let (_root, store) = store_with_publication();
        let mut publisher = RecordingPublisher {
            outcomes: VecDeque::from([
                Err(
                    ControlPlaneError::StagingEvidencePublicationOutcomeUnconfirmed {
                        message: "response lost".to_owned(),
                    },
                ),
                Ok(()),
            ]),
            pages: Vec::new(),
        };

        assert!(matches!(
            run_outbox_step(&store, &mut publisher, 100),
            OutboxStep::Deferred { .. }
        ));
        assert!(matches!(
            run_outbox_step(&store, &mut publisher, 200),
            OutboxStep::Published { generation: 1 }
        ));
        assert_eq!(publisher.pages.len(), 2);
        assert_eq!(publisher.pages[0], publisher.pages[1]);
        assert!(matches!(
            run_outbox_step(&store, &mut publisher, 300),
            OutboxStep::Idle
        ));
    }

    #[test]
    fn fatal_response_does_not_acknowledge_the_durable_page() {
        let (_root, store) = store_with_publication();
        let mut publisher = RecordingPublisher {
            outcomes: VecDeque::from([Err(ControlPlaneError::CommandDecode {
                message: "conflicting evidence page".to_owned(),
            })]),
            pages: Vec::new(),
        };

        assert!(matches!(
            run_outbox_step(&store, &mut publisher, 100),
            OutboxStep::Fatal { .. }
        ));
        let retained = store.next_evidence_page().unwrap().unwrap();
        assert_eq!(retained, publisher.pages[0]);
    }

    #[test]
    fn publisher_panic_invokes_the_process_fatal_callback() {
        let (_root, store) = store_with_publication();
        let (fatal_tx, fatal_rx) = mpsc::sync_channel(1);
        let mut outbox = StorageNodeMetadataTransferStagingOutbox::spawn(
            Arc::new(store),
            PanickingPublisher,
            || 100,
            move |error| fatal_tx.send(error).unwrap(),
        )
        .unwrap();

        let error = fatal_rx
            .recv_timeout(Duration::from_secs(1))
            .expect("publisher panic did not invoke the fatal callback");
        assert!(error.contains("outbox panicked"));
        assert!(outbox.status().failed);
        outbox.stop();
    }
}
