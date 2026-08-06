pub struct PayloadShardWriteTestHookGuard;
pub struct PayloadShardReadTestHookGuard;
pub struct PayloadCleanupTestHookGuard;
pub type PayloadShardWriteTestHook = fn();
pub type PayloadShardReadTestHook = fn();
pub type PayloadShardCleanupTestHook = fn();
pub type PayloadCleanupErrorTestHook = fn();

impl PayloadShardWriteTestHookGuard {
    pub fn test_install_before_placed_payload_shard_write_hook(&self) {}
    pub fn test_install_before_placed_payload_shard_read_hook(&self) {}
    pub fn test_install_before_placed_payload_shard_delete_hook(&self) {}
    pub fn test_install_before_metadata_primary_payload_ack_delete_hook(&self) {}
    pub fn test_install_best_effort_payload_cleanup_error_hook(&self) {}
    pub(crate) fn test_object_payload_shard_file_matches_ack(&self) {}
}
