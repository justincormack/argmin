// Copyright The Argmin Authors.
// SPDX-License-Identifier: Apache-2.0

use std::sync::{Arc, Mutex};

use crate::cluster::{PayloadShardWriteTestHookGuard, ShardLocation};
use crate::{ShardKey, StorageCluster, StoreError, StoreFailure};

/// Opaque storage-owned failure injected at the physical shard-write boundary.
pub struct TestPayloadShardWriteFailure {
    error: StoreError,
}

/// Construct a retryable-convergence failure without exposing its physical
/// storage representation to the caller.
#[must_use]
pub fn payload_shard_write_retryable_convergence_failure() -> TestPayloadShardWriteFailure {
    TestPayloadShardWriteFailure {
        error: StoreError::RouteMapExpired {
            cluster_epoch: crate::ClusterEpoch::INITIAL,
            valid_until_ms: 1,
            now_ms: 2,
        },
    }
}

/// Storage-owned callback invoked before each physical shard-write attempt.
///
/// The callback receives only the one-based attempt number. Physical shard
/// placement and identity remain private to storage.
pub type TestPayloadShardWriteAttemptHook =
    Arc<dyn Fn(usize) -> Result<(), TestPayloadShardWriteFailure> + Send + Sync>;

/// Active physical shard-write observation scoped to one storage cluster.
///
/// Dropping this guard removes the underlying hook. Call [`Self::finish`] to
/// retain opaque evidence for cleanup assertions after removing the hook.
pub struct TestPayloadShardWriteAttemptGuard {
    hook_guard: Option<PayloadShardWriteTestHookGuard>,
    cluster: Arc<StorageCluster>,
    attempted: Arc<Mutex<Vec<(ShardLocation, ShardKey)>>>,
}

impl TestPayloadShardWriteAttemptGuard {
    #[must_use]
    pub fn finish(mut self) -> TestPayloadShardWriteAttempts {
        self.hook_guard.take();
        TestPayloadShardWriteAttempts {
            cluster: Arc::clone(&self.cluster),
            attempted: Arc::clone(&self.attempted),
        }
    }
}

/// Opaque evidence of the physical shard writes attempted by one operation.
pub struct TestPayloadShardWriteAttempts {
    cluster: Arc<StorageCluster>,
    attempted: Arc<Mutex<Vec<(ShardLocation, ShardKey)>>>,
}

impl TestPayloadShardWriteAttempts {
    /// Return how many shard writes reached the storage effect boundary.
    #[must_use]
    pub fn count(&self) -> usize {
        self.attempted
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .len()
    }

    /// Verify that neither durable acknowledgement rows nor shard files remain
    /// for any attempted write in the originating storage cluster.
    pub fn all_absent(&self) -> Result<bool, StoreFailure> {
        let attempted = self
            .attempted
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        for (location, key) in attempted.iter() {
            if self
                .cluster
                .test_placed_payload_shard_row_exists(*location, key)
                .map_err(StoreFailure::from)?
                || self
                    .cluster
                    .test_placed_payload_shard_file_exists(*location, key)
                    .map_err(StoreFailure::from)?
            {
                return Ok(false);
            }
        }
        Ok(true)
    }
}

/// Install a shard-write hook without exposing physical placement to the
/// caller.
pub fn install_payload_shard_write_attempt_hook(
    cluster: &Arc<StorageCluster>,
    hook: TestPayloadShardWriteAttemptHook,
) -> TestPayloadShardWriteAttemptGuard {
    let attempted = Arc::new(Mutex::new(Vec::new()));
    let attempted_for_hook = Arc::clone(&attempted);
    let hook_guard = cluster.test_install_before_placed_payload_shard_write_hook(Arc::new(
        move |location, key| {
            let attempt = {
                let mut attempted = attempted_for_hook
                    .lock()
                    .unwrap_or_else(|poisoned| poisoned.into_inner());
                attempted.push((*location, key.clone()));
                attempted.len()
            };
            hook(attempt).map_err(|failure| failure.error)
        },
    ));
    TestPayloadShardWriteAttemptGuard {
        hook_guard: Some(hook_guard),
        cluster: Arc::clone(cluster),
        attempted,
    }
}
