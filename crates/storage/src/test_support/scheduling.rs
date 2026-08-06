use std::any::Any;
use std::sync::Arc;

use crate::StorageCluster;

/// A deterministic action invoked at a storage-owned scheduling boundary.
///
/// The callback intentionally receives no storage identity or mutable state.
/// Cross-crate tests can coordinate an interleaving while the hook registry,
/// command IDs, PGs, and payload placement remain owned by storage.
pub type TestStorageSchedulingAction = Arc<dyn Fn() + Send + Sync>;

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
/// Each method installs a no-argument action at the named production boundary.
/// The underlying hook registries and their operation-specific guards remain
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
