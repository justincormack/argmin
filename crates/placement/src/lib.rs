pub mod cluster;
pub mod config;
pub mod constraint;
mod hash;
mod placer;
pub mod topology;

pub use cluster::{ClusterMap, NodeId, NodeInfo};
pub use config::{PlacementConfig, PlacementError};
pub use constraint::{Admission, PlacementConstraint};
pub use placer::Placer;
pub use topology::{Level, TopologyError, TopologyKey};

/// Maximum number of shards supported by the placement engine.
/// Stack arrays in `place()` are sized to this constant.
pub(crate) const MAX_SHARDS: usize = 32;

#[cfg(test)]
mod tests;
