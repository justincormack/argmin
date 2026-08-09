// Copyright The Argmin Authors.
// SPDX-License-Identifier: Apache-2.0

use std::any::Any;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use crate::{ClusterEpoch, ObjectPgActionError, StorageCluster, StoreError};

/// Opaque guard for a storage-owned deterministic failure scenario.
pub struct TestStorageFailureGuard {
    _inner: Box<dyn Any>,
    invocations: Arc<AtomicUsize>,
}

impl TestStorageFailureGuard {
    fn new<T: 'static>(inner: T, invocations: Arc<AtomicUsize>) -> Self {
        Self {
            _inner: Box::new(inner),
            invocations,
        }
    }

    /// Number of times the selected production boundary was reached.
    pub fn invocation_count(&self) -> usize {
        self.invocations.load(Ordering::SeqCst)
    }
}

/// Storage-owned failure scenarios for cross-crate recovery tests.
///
/// These helpers expose only the semantic failure needed by the caller. Raw
/// storage errors, route coordinates, hook callbacks, and hook registries stay
/// inside the storage crate.
pub trait StorageClusterFailureTestSupport {
    /// Fail every retained stream abort with retryable metadata contention.
    fn test_fail_retained_stream_abort_with_contention(&self) -> TestStorageFailureGuard;

    /// Fail the first `failure_count` retained stream aborts with retryable
    /// metadata contention, then allow subsequent attempts to proceed.
    fn test_fail_retained_stream_abort_with_contention_for_attempts(
        &self,
        failure_count: usize,
    ) -> TestStorageFailureGuard;

    /// Fail immediately after durable reclaim-claim acquisition with an
    /// expired-route error.
    fn test_fail_route_after_reclaim_claim_acquired(&self) -> TestStorageFailureGuard;

    /// Fail reclaim ownership lookup with a storage I/O error.
    fn test_fail_reclaim_ownership_lookup(&self) -> TestStorageFailureGuard;

    /// Fail reclaim claim release with a storage I/O error.
    fn test_fail_reclaim_claim_release(&self) -> TestStorageFailureGuard;

    /// Fail every payload-shard read with storage-node resource exhaustion.
    fn test_fail_payload_shard_reads_with_resource_exhaustion(&self) -> TestStorageFailureGuard;

    /// Fail the next payload-shard read used by repair with storage-node
    /// resource exhaustion, then allow later reads to proceed.
    fn test_fail_next_repair_payload_shard_read_with_resource_exhaustion(
        &self,
    ) -> TestStorageFailureGuard;

    /// Fail placed payload-shard deletion and observe the corresponding
    /// best-effort cleanup diagnostic.
    fn test_fail_placed_payload_shard_cleanup(&self) -> TestStorageFailureGuard;

    /// Fail payload acknowledgement deletion and observe the corresponding
    /// best-effort cleanup diagnostic.
    fn test_fail_payload_ack_cleanup(&self) -> TestStorageFailureGuard;
}

fn retained_stream_contention() -> ObjectPgActionError {
    ObjectPgActionError::Store(StoreError::MetadataCommandContention {
        context: "injected retained stream abort contention",
    })
}

fn injected_reclaim_io(context: &'static str) -> ObjectPgActionError {
    ObjectPgActionError::Store(StoreError::Io {
        context,
        source: std::io::Error::other(context),
    })
}

fn injected_cleanup_io(context: &'static str) -> StoreError {
    StoreError::Io {
        context,
        source: std::io::Error::other(context),
    }
}

fn require_expected_cleanup_error(
    operation: &'static str,
    error: &StoreError,
    expected_operation: &'static str,
    expected_context: &'static str,
) {
    assert_eq!(operation, expected_operation);
    match error {
        StoreError::Io { context, .. } => assert_eq!(*context, expected_context),
        other => panic!("expected injected cleanup I/O error, got {other:?}"),
    }
}

impl StorageClusterFailureTestSupport for StorageCluster {
    fn test_fail_retained_stream_abort_with_contention(&self) -> TestStorageFailureGuard {
        let invocations = Arc::new(AtomicUsize::new(0));
        let hook_invocations = Arc::clone(&invocations);
        let guard = self.test_install_before_retained_stream_abort_hook(Arc::new(move || {
            hook_invocations.fetch_add(1, Ordering::SeqCst);
            Err(retained_stream_contention())
        }));
        TestStorageFailureGuard::new(guard, invocations)
    }

    fn test_fail_retained_stream_abort_with_contention_for_attempts(
        &self,
        failure_count: usize,
    ) -> TestStorageFailureGuard {
        let invocations = Arc::new(AtomicUsize::new(0));
        let hook_invocations = Arc::clone(&invocations);
        let remaining = Arc::new(AtomicUsize::new(failure_count));
        let hook_remaining = Arc::clone(&remaining);
        let guard = self.test_install_before_retained_stream_abort_hook(Arc::new(move || {
            hook_invocations.fetch_add(1, Ordering::SeqCst);
            if hook_remaining
                .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |remaining| {
                    remaining.checked_sub(1)
                })
                .is_ok()
            {
                Err(retained_stream_contention())
            } else {
                Ok(())
            }
        }));
        TestStorageFailureGuard::new(guard, invocations)
    }

    fn test_fail_route_after_reclaim_claim_acquired(&self) -> TestStorageFailureGuard {
        let invocations = Arc::new(AtomicUsize::new(0));
        let hook_invocations = Arc::clone(&invocations);
        let guard = self.test_install_after_reclaim_claim_acquired_hook(Arc::new(move || {
            hook_invocations.fetch_add(1, Ordering::SeqCst);
            Err(ObjectPgActionError::Store(StoreError::RouteMapExpired {
                cluster_epoch: ClusterEpoch::INITIAL,
                valid_until_ms: 1,
                now_ms: 2,
            }))
        }));
        TestStorageFailureGuard::new(guard, invocations)
    }

    fn test_fail_reclaim_ownership_lookup(&self) -> TestStorageFailureGuard {
        let invocations = Arc::new(AtomicUsize::new(0));
        let hook_invocations = Arc::clone(&invocations);
        let guard = self.test_install_before_reclaim_ownership_lookup_hook(Arc::new(move || {
            hook_invocations.fetch_add(1, Ordering::SeqCst);
            Err(injected_reclaim_io(
                "injected reclaim ownership lookup failure",
            ))
        }));
        TestStorageFailureGuard::new(guard, invocations)
    }

    fn test_fail_reclaim_claim_release(&self) -> TestStorageFailureGuard {
        let invocations = Arc::new(AtomicUsize::new(0));
        let hook_invocations = Arc::clone(&invocations);
        let guard = self.test_install_before_reclaim_claim_release_hook(Arc::new(move || {
            hook_invocations.fetch_add(1, Ordering::SeqCst);
            Err(injected_reclaim_io(
                "injected reclaim claim release failure",
            ))
        }));
        TestStorageFailureGuard::new(guard, invocations)
    }

    fn test_fail_payload_shard_reads_with_resource_exhaustion(&self) -> TestStorageFailureGuard {
        let invocations = Arc::new(AtomicUsize::new(0));
        let hook_invocations = Arc::clone(&invocations);
        let guard = self.test_install_before_placed_payload_shard_read_hook(Arc::new(
            move |location, _| {
                hook_invocations.fetch_add(1, Ordering::SeqCst);
                Err(StoreError::storage_node_resource_exhausted(
                    location.node_id().as_u32(),
                    "read payload shard",
                ))
            },
        ));
        TestStorageFailureGuard::new(guard, invocations)
    }

    fn test_fail_next_repair_payload_shard_read_with_resource_exhaustion(
        &self,
    ) -> TestStorageFailureGuard {
        let invocations = Arc::new(AtomicUsize::new(0));
        let hook_invocations = Arc::clone(&invocations);
        let remaining = Arc::new(AtomicUsize::new(1));
        let hook_remaining = Arc::clone(&remaining);
        let guard = self.test_install_before_placed_payload_shard_read_hook(Arc::new(
            move |location, _| {
                hook_invocations.fetch_add(1, Ordering::SeqCst);
                if hook_remaining
                    .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |remaining| {
                        remaining.checked_sub(1)
                    })
                    .is_ok()
                {
                    Err(StoreError::storage_node_resource_exhausted(
                        location.node_id().as_u32(),
                        "repair read payload shard",
                    ))
                } else {
                    Ok(())
                }
            },
        ));
        TestStorageFailureGuard::new(guard, invocations)
    }

    fn test_fail_placed_payload_shard_cleanup(&self) -> TestStorageFailureGuard {
        const OPERATION: &str = "delete placed payload shard";
        const CONTEXT: &str = "injected placed cleanup delete failure";

        let invocations = Arc::new(AtomicUsize::new(0));
        let observer_invocations = Arc::clone(&invocations);
        let observer = self.test_install_best_effort_payload_cleanup_error_hook(Arc::new(
            move |operation, error| {
                require_expected_cleanup_error(operation, error, OPERATION, CONTEXT);
                observer_invocations.fetch_add(1, Ordering::SeqCst);
            },
        ));
        let failure = self.test_install_before_placed_payload_shard_delete_hook(Arc::new(|_| {
            Err(injected_cleanup_io(CONTEXT))
        }));
        // Tuple fields drop left-to-right. Remove the failure injector before
        // its observer so concurrent cleanup cannot emit an unobserved
        // injected error while this guard is being released.
        TestStorageFailureGuard::new((failure, observer), invocations)
    }

    fn test_fail_payload_ack_cleanup(&self) -> TestStorageFailureGuard {
        const OPERATION: &str = "delete payload ack";
        const CONTEXT: &str = "injected ack cleanup delete failure";

        let invocations = Arc::new(AtomicUsize::new(0));
        let observer_invocations = Arc::clone(&invocations);
        let observer = self.test_install_best_effort_payload_cleanup_error_hook(Arc::new(
            move |operation, error| {
                require_expected_cleanup_error(operation, error, OPERATION, CONTEXT);
                observer_invocations.fetch_add(1, Ordering::SeqCst);
            },
        ));
        let failure =
            self.test_install_before_metadata_primary_payload_ack_delete_hook(Arc::new(|_| {
                Err(injected_cleanup_io(CONTEXT))
            }));
        // Keep the observer installed until after the failure injector has
        // been removed; tuple fields drop left-to-right.
        TestStorageFailureGuard::new((failure, observer), invocations)
    }
}
