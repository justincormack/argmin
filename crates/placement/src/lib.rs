pub mod topology;
pub mod cluster;
pub mod config;
pub mod constraint;
mod hash;
mod placer;

pub use topology::{Level, TopologyKey, TopologyError};
pub use cluster::{NodeId, NodeInfo, ClusterMap};
pub use config::{PlacementConfig, PlacementError};
pub use constraint::{Admission, PlacementConstraint};
pub use placer::Placer;

/// Maximum number of shards supported by the placement engine.
/// Stack arrays in `place()` are sized to this constant.
pub(crate) const MAX_SHARDS: usize = 32;

#[cfg(test)]
mod tests;
