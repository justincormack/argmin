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
}
