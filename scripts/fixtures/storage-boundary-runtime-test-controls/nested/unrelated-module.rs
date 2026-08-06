pub static TIME_OVERRIDE_MILLIS: u64 = 0;
pub static MONOTONIC_TIME_OVERRIDE_MILLIS: u64 = 0;
pub struct TestTimeOverrideGuard;
pub struct TestTimeOverrideControl;
pub fn test_time_override_guard() {}
pub fn with_time_and_monotonic_override() {}

pub struct ForeignTopology;

impl ForeignTopology {
    pub fn test_topology_generation(&self) {}
    pub fn test_raft_voters(&self) {}
    pub fn test_logical_acting_sets(&self) {}
}
