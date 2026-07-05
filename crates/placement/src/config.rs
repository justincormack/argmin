use thiserror::Error;

/// Parameters for one placement scheme. Cheap to copy.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PlacementConfig {
    /// Total shards to place per stripe (k + m in EC terms).
    total_shards: usize,
}

impl PlacementConfig {
    /// Create a new PlacementConfig.
    ///
    /// Returns Err(InvalidTotalShards) if total_shards == 0 or total_shards > MAX_SHARDS (32).
    pub fn new(total_shards: usize) -> Result<Self, PlacementError> {
        Self::validate_total_shards(total_shards)?;
        Ok(PlacementConfig { total_shards })
    }

    pub(crate) fn validate(self) -> Result<Self, PlacementError> {
        Self::validate_total_shards(self.total_shards)?;
        Ok(self)
    }

    fn validate_total_shards(total_shards: usize) -> Result<(), PlacementError> {
        if total_shards == 0 || total_shards > crate::MAX_SHARDS {
            return Err(PlacementError::InvalidTotalShards);
        }
        Ok(())
    }

    /// Total shards to place per stripe.
    #[must_use]
    pub const fn total_shards(self) -> usize {
        self.total_shards
    }
}

#[derive(Debug, Error, PartialEq)]
pub enum PlacementError {
    #[error("empty cluster: no nodes provided")]
    EmptyCluster,

    #[error("duplicate node id {id}")]
    DuplicateNodeId { id: u32 },

    #[error("invalid weight {weight} for node {id}: must be finite and non-negative")]
    InvalidWeight { id: u32, weight: f64 },

    #[error("total_shards must be between 1 and 32")]
    InvalidTotalShards,

    #[error("cannot place {shards} shards: only {nodes} active nodes available")]
    TooFewNodes { shards: usize, nodes: usize },

    #[error("constraint prevented filling all {shards} slots: only {filled} satisfiable")]
    ConstraintUnsatisfiable { shards: usize, filled: usize },

    #[error("output slice length {got} != total_shards {expected}")]
    OutputLengthMismatch { got: usize, expected: usize },
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn valid_config() {
        assert!(PlacementConfig::new(6).is_ok());
        assert!(PlacementConfig::new(1).is_ok());
        assert!(PlacementConfig::new(32).is_ok());
    }

    #[test]
    fn invalid_total_shards_zero() {
        assert_eq!(
            PlacementConfig::new(0),
            Err(PlacementError::InvalidTotalShards)
        );
    }

    #[test]
    fn invalid_total_shards_too_large() {
        assert_eq!(
            PlacementConfig::new(33),
            Err(PlacementError::InvalidTotalShards)
        );
    }

    #[test]
    fn invalid_total_shards_max_u8() {
        assert_eq!(
            PlacementConfig::new(255),
            Err(PlacementError::InvalidTotalShards)
        );
    }
}
