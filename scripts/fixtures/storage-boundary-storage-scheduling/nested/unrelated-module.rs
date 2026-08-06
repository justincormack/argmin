pub struct BucketPgTestGuard;

pub type DirectPutMetadataPublishHook = ();
pub type ObjectMetadataCommandPublishHook = ();
pub static AFTER_DIRECT_PUT_METADATA_PUBLISH_HOOKS: () = ();
pub static AFTER_OBJECT_METADATA_COMMAND_PUBLISH_HOOKS: () = ();

impl BucketPgTestGuard {
    pub fn test_lock_bucket_pg(&self) {}
    pub fn test_install_after_direct_put_metadata_publish_hook(&self) {}
    pub(crate) fn test_install_after_object_metadata_command_publish_hook(&self) {}
}
