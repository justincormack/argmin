use crate::StorageShardRepairSweeper;

// This models an additional inherent impl placed outside the current
// maintenance implementation modules. A source-root scan must still find it.
impl StorageShardRepairSweeper {
    pub fn test_repair_one_pending(&self) -> bool {
        false
    }
}
