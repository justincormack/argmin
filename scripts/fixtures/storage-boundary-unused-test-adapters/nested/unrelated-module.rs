pub struct UnreviewedAdapter;

impl UnreviewedAdapter {
    pub(crate) fn test_crate_private_adapter(&self) {}

    #[doc(hidden)]
    pub fn test_unreviewed_storage_adapter(&self) {}
}
