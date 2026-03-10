//! Shared S3/domain value types used across storage and server layers.

use std::num::NonZeroU64;

/// Bucket versioning state.
#[repr(u8)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BucketVersioningState {
    Disabled = 0,
    Enabled = 1,
    Suspended = 2,
}

impl BucketVersioningState {
    #[must_use]
    pub fn from_u8(v: u8) -> Option<Self> {
        match v {
            0 => Some(Self::Disabled),
            1 => Some(Self::Enabled),
            2 => Some(Self::Suspended),
            _ => None,
        }
    }
}

/// Object version identifier.
///
/// `Null` represents the single unversioned copy (`version_id=0` in storage).
/// `Versioned` represents an explicit version (`version_id>=1` in storage).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum VersionId {
    /// The null version (unversioned bucket).
    Null,
    /// An explicit version in a versioning-enabled bucket.
    Versioned(NonZeroU64),
}

impl VersionId {
    /// Convert from the raw `u64` stored in the database.
    #[must_use]
    pub fn from_u64(v: u64) -> Self {
        match NonZeroU64::new(v) {
            Some(nz) => Self::Versioned(nz),
            None => Self::Null,
        }
    }

    /// Convert to the raw `u64` for database storage.
    #[must_use]
    pub fn to_u64(self) -> u64 {
        match self {
            Self::Null => 0,
            Self::Versioned(v) => v.get(),
        }
    }

    /// Returns true if this is the null (unversioned) version.
    #[must_use]
    pub fn is_null(self) -> bool {
        matches!(self, Self::Null)
    }

    /// Returns true if this is an explicit versioned ID.
    #[must_use]
    pub fn is_versioned(self) -> bool {
        matches!(self, Self::Versioned(_))
    }
}

impl std::fmt::Display for VersionId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Null => f.write_str("null"),
            Self::Versioned(v) => write!(f, "{v}"),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{BucketVersioningState, VersionId};

    #[test]
    fn bucket_versioning_state_from_u8_round_trip() {
        assert_eq!(
            BucketVersioningState::from_u8(0),
            Some(BucketVersioningState::Disabled)
        );
        assert_eq!(
            BucketVersioningState::from_u8(1),
            Some(BucketVersioningState::Enabled)
        );
        assert_eq!(
            BucketVersioningState::from_u8(2),
            Some(BucketVersioningState::Suspended)
        );
        assert_eq!(BucketVersioningState::from_u8(3), None);
    }

    #[test]
    fn version_id_round_trip() {
        assert_eq!(VersionId::from_u64(0), VersionId::Null);
        assert_eq!(VersionId::Null.to_u64(), 0);

        let versioned = VersionId::from_u64(42);
        assert!(versioned.is_versioned());
        assert_eq!(versioned.to_u64(), 42);
        assert_eq!(versioned.to_string(), "42");
    }

    #[test]
    fn version_id_null_helpers() {
        assert!(VersionId::Null.is_null());
        assert!(!VersionId::Null.is_versioned());
        assert_eq!(VersionId::Null.to_string(), "null");
    }
}
