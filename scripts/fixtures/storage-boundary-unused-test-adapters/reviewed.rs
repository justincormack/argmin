pub struct ReviewedAdapter;

impl ReviewedAdapter {
    pub fn test_ec_scratch_allocation_count(&self) {}

    pub async fn test_async_adapter(&self) {}

    pub const fn test_const_adapter(&self) {}

    pub unsafe fn test_unsafe_adapter(&self) {}

    pub extern "C" fn test_extern_adapter(&self) {}
}
