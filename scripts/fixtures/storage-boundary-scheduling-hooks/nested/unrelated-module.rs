pub struct StreamAbortTestHookGuard;
pub struct RetainedStreamAbortTestHookGuard;
pub struct ReclaimOwnershipLookupTestHookGuard;
pub struct ReclaimClaimAcquiredTestHookGuard;
pub struct ReclaimClaimReleaseTestHookGuard;
pub type RetainedStreamAbortHook = fn();
pub type ReclaimCoordinationTestHook = fn();

pub struct ForeignCluster;

impl ForeignCluster {
    pub fn test_install_before_stream_abort_storage_hook(&self) {}

    pub fn test_install_before_metadata_command_pending_install_hook(&self) {}

    pub fn test_install_before_stream_put_finalize_command_id_hook(&self) {}

    pub fn test_install_before_retained_stream_abort_hook(&self) {}

    pub fn test_install_before_reclaim_ownership_lookup_hook(&self) {}

    pub fn test_install_after_reclaim_claim_acquired_hook(&self) {}

    pub fn test_install_before_reclaim_claim_release_hook(&self) {}
}
