// Copyright The Argmin Authors.
// SPDX-License-Identifier: Apache-2.0

use std::any::Any;
use std::sync::Arc;

use crate::StorageCluster;

use super::TestInjectedStorageFailure;

/// A deterministic action invoked at a storage-owned scheduling boundary.
///
/// The callback intentionally receives no storage identity or mutable state.
/// Cross-crate tests can coordinate an interleaving while the hook registry,
/// command IDs, PGs, and payload placement remain owned by storage.
pub type TestStorageSchedulingAction = Arc<dyn Fn() + Send + Sync>;

pub type TestStorageFallibleSchedulingAction =
    Arc<dyn Fn() -> Result<(), TestInjectedStorageFailure> + Send + Sync>;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum TestBucketDeletePostReservationProgress {
    MoreFrontiersRemain,
    FinalFrontierRecorded,
}

pub type TestBucketDeletePostReservationProgressAction = Arc<
    dyn Fn(TestBucketDeletePostReservationProgress) -> Result<(), TestInjectedStorageFailure>
        + Send
        + Sync,
>;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum TestBucketDeleteExactDrainStart {
    Fresh,
    ResumedFromDurableProgress,
}

pub type TestBucketDeleteExactDrainSchedulingAction = Arc<
    dyn Fn(TestBucketDeleteExactDrainStart) -> Result<(), TestInjectedStorageFailure> + Send + Sync,
>;

/// Opaque lifetime guard for an installed storage scheduling action.
pub struct TestStorageSchedulingGuard {
    _inner: Box<dyn Any>,
}

impl TestStorageSchedulingGuard {
    fn new<T: 'static>(inner: T) -> Self {
        Self {
            _inner: Box::new(inner),
        }
    }
}

/// Curated deterministic scheduling boundaries for cross-crate tests.
///
/// Most methods install a no-argument action. Bucket-delete progress methods
/// expose only semantic phase/frontier state, never raw PG identities. The
/// underlying hook registries and their operation-specific guards remain
/// crate-private.
pub trait StorageClusterSchedulingTestSupport {
    fn test_install_before_stream_abort_storage_hook(
        &self,
        action: TestStorageSchedulingAction,
    ) -> TestStorageSchedulingGuard;

    fn test_install_before_retained_stream_cleanup_capability_hook(
        &self,
        action: TestStorageSchedulingAction,
    ) -> TestStorageSchedulingGuard;

    fn test_install_after_retained_stream_cleanup_capability_hook(
        &self,
        action: TestStorageSchedulingAction,
    ) -> TestStorageSchedulingGuard;

    fn test_install_before_metadata_command_pending_install_hook(
        &self,
        action: TestStorageSchedulingAction,
    ) -> TestStorageSchedulingGuard;

    fn test_install_before_stream_append_command_id_hook(
        &self,
        action: TestStorageSchedulingAction,
    ) -> TestStorageSchedulingGuard;

    fn test_install_before_stream_put_create_pending_install_hook(
        &self,
        action: TestStorageSchedulingAction,
    ) -> TestStorageSchedulingGuard;

    fn test_install_before_stream_put_finalize_command_id_hook(
        &self,
        action: TestStorageSchedulingAction,
    ) -> TestStorageSchedulingGuard;

    fn test_install_before_bucket_delete_final_visibility_hook(
        &self,
        action: TestStorageFallibleSchedulingAction,
    ) -> TestStorageSchedulingGuard;

    fn test_install_after_bucket_delete_final_visibility_proven_hook(
        &self,
        action: TestStorageFallibleSchedulingAction,
    ) -> TestStorageSchedulingGuard;

    fn test_install_after_bucket_delete_post_reservation_progress_hook(
        &self,
        action: TestBucketDeletePostReservationProgressAction,
    ) -> TestStorageSchedulingGuard;

    fn test_install_before_bucket_delete_exact_drain_hook(
        &self,
        action: TestBucketDeleteExactDrainSchedulingAction,
    ) -> TestStorageSchedulingGuard;

    /// Run an identity-free action immediately before each payload-shard read.
    fn test_install_before_payload_shard_read_action(
        &self,
        action: TestStorageSchedulingAction,
    ) -> TestStorageSchedulingGuard;
}

impl StorageClusterSchedulingTestSupport for StorageCluster {
    fn test_install_before_stream_abort_storage_hook(
        &self,
        action: TestStorageSchedulingAction,
    ) -> TestStorageSchedulingGuard {
        TestStorageSchedulingGuard::new(
            StorageCluster::test_install_before_stream_abort_storage_hook(self, action),
        )
    }

    fn test_install_before_retained_stream_cleanup_capability_hook(
        &self,
        action: TestStorageSchedulingAction,
    ) -> TestStorageSchedulingGuard {
        TestStorageSchedulingGuard::new(
            StorageCluster::test_install_before_retained_stream_cleanup_capability_hook(
                self, action,
            ),
        )
    }

    fn test_install_after_retained_stream_cleanup_capability_hook(
        &self,
        action: TestStorageSchedulingAction,
    ) -> TestStorageSchedulingGuard {
        TestStorageSchedulingGuard::new(
            StorageCluster::test_install_after_retained_stream_cleanup_capability_hook(
                self, action,
            ),
        )
    }

    fn test_install_before_metadata_command_pending_install_hook(
        &self,
        action: TestStorageSchedulingAction,
    ) -> TestStorageSchedulingGuard {
        TestStorageSchedulingGuard::new(
            StorageCluster::test_install_before_metadata_command_pending_install_hook(self, action),
        )
    }

    fn test_install_before_stream_append_command_id_hook(
        &self,
        action: TestStorageSchedulingAction,
    ) -> TestStorageSchedulingGuard {
        TestStorageSchedulingGuard::new(
            StorageCluster::test_install_before_stream_append_command_id_hook(self, action),
        )
    }

    fn test_install_before_stream_put_create_pending_install_hook(
        &self,
        action: TestStorageSchedulingAction,
    ) -> TestStorageSchedulingGuard {
        TestStorageSchedulingGuard::new(
            StorageCluster::test_install_before_stream_put_create_pending_install_hook(
                self, action,
            ),
        )
    }

    fn test_install_before_stream_put_finalize_command_id_hook(
        &self,
        action: TestStorageSchedulingAction,
    ) -> TestStorageSchedulingGuard {
        TestStorageSchedulingGuard::new(
            StorageCluster::test_install_before_stream_put_finalize_command_id_hook(self, action),
        )
    }

    fn test_install_before_bucket_delete_final_visibility_hook(
        &self,
        action: TestStorageFallibleSchedulingAction,
    ) -> TestStorageSchedulingGuard {
        TestStorageSchedulingGuard::new(
            StorageCluster::test_install_before_bucket_delete_final_visibility_hook(
                self,
                Arc::new(move || action().map_err(TestInjectedStorageFailure::into_store_error)),
            ),
        )
    }

    fn test_install_after_bucket_delete_final_visibility_proven_hook(
        &self,
        action: TestStorageFallibleSchedulingAction,
    ) -> TestStorageSchedulingGuard {
        TestStorageSchedulingGuard::new(
            StorageCluster::test_install_after_bucket_delete_final_visibility_proven_hook(
                self,
                Arc::new(move || action().map_err(TestInjectedStorageFailure::into_store_error)),
            ),
        )
    }

    fn test_install_after_bucket_delete_post_reservation_progress_hook(
        &self,
        action: TestBucketDeletePostReservationProgressAction,
    ) -> TestStorageSchedulingGuard {
        TestStorageSchedulingGuard::new(
            StorageCluster::test_install_after_bucket_delete_semantic_post_reservation_progress_hook(
                self,
                Arc::new(move |more_frontiers_remain| {
                    let progress = if more_frontiers_remain {
                        TestBucketDeletePostReservationProgress::MoreFrontiersRemain
                    } else {
                        TestBucketDeletePostReservationProgress::FinalFrontierRecorded
                    };
                    action(progress).map_err(TestInjectedStorageFailure::into_store_error)
                }),
            ),
        )
    }

    fn test_install_before_bucket_delete_exact_drain_hook(
        &self,
        action: TestBucketDeleteExactDrainSchedulingAction,
    ) -> TestStorageSchedulingGuard {
        TestStorageSchedulingGuard::new(
            StorageCluster::test_install_before_bucket_delete_exact_drain_hook(
                self,
                Arc::new(move |has_durable_progress, _next_object_pg_id| {
                    let start = if has_durable_progress {
                        TestBucketDeleteExactDrainStart::ResumedFromDurableProgress
                    } else {
                        TestBucketDeleteExactDrainStart::Fresh
                    };
                    action(start).map_err(TestInjectedStorageFailure::into_store_error)
                }),
            ),
        )
    }

    fn test_install_before_payload_shard_read_action(
        &self,
        action: TestStorageSchedulingAction,
    ) -> TestStorageSchedulingGuard {
        TestStorageSchedulingGuard::new(
            StorageCluster::test_install_before_placed_payload_shard_read_hook(
                self,
                Arc::new(move |_, _| {
                    action();
                    Ok(())
                }),
            ),
        )
    }
}
