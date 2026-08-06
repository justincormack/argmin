pub struct StreamAbortTestHookGuard;

pub struct ForeignCluster;

impl ForeignCluster {
    pub fn test_install_before_stream_abort_storage_hook(&self) {}

    pub fn test_install_before_metadata_command_pending_install_hook(&self) {}

    pub fn test_install_before_stream_put_finalize_command_id_hook(&self) {}
}
