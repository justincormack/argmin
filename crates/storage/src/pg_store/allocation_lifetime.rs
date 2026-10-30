// Copyright The Argmin Authors.
// SPDX-License-Identifier: Apache-2.0

//! Local identity for the future replicated staging-window issuer binding.
//!
//! This is NOT a write grant. No allocator consumes it yet: route admission,
//! durable window coverage, publication confirmation and physical exclusion are
//! still required. It is deliberately absent from checkpoints and SQL state.

use std::cell::Cell;

use crate::error::StoreError;

#[derive(Clone, Copy, PartialEq, Eq)]
enum State {
    Active([u8; 16]),
    Replacing([u8; 16]),
    Retired,
}

pub(super) struct PgAllocationLifetime {
    state: Cell<State>,
}

fn fresh_identity() -> Result<[u8; 16], StoreError> {
    let mut identity = [0; 16];
    argmin_crypto::random::fill(&mut identity).map_err(|error| StoreError::Io {
        context: "mint PG allocation lifetime",
        source: std::io::Error::other(error.to_string()),
    })?;
    Ok(identity)
}

impl PgAllocationLifetime {
    /// Inspection-only stores do not own an allocation lifetime.
    pub(super) fn retired() -> Self {
        Self {
            state: Cell::new(State::Retired),
        }
    }

    pub(super) fn new() -> Result<Self, StoreError> {
        Ok(Self {
            state: Cell::new(State::Active(fresh_identity()?)),
        })
    }

    /// Called under PG serialization, before attempting a state replacement.
    /// Even failed replacement must not restore prior allocation authority.
    /// Generate the candidate before SQL mutation, never after durable commit.
    pub(super) fn begin_replacement(&self) -> Result<ReplacementLifetime<'_>, StoreError> {
        self.state.set(State::Retired);
        let identity = fresh_identity()?;
        self.state.set(State::Replacing(identity));
        Ok(ReplacementLifetime {
            lifetime: self,
            identity,
        })
    }

    #[cfg(test)]
    pub(super) fn active_identity(&self) -> Option<[u8; 16]> {
        match self.state.get() {
            State::Active(identity) => Some(identity),
            State::Replacing(_) | State::Retired => None,
        }
    }
}

/// Only the successful replacement path consumes this guard. Errors, unwind,
/// and commit uncertainty leave allocation retired, independently of SQL rollback.
pub(in crate::node_runtime) struct ReplacementLifetime<'a> {
    lifetime: &'a PgAllocationLifetime,
    identity: [u8; 16],
}

impl ReplacementLifetime<'_> {
    pub(in crate::node_runtime) fn complete(self) -> Result<(), StoreError> {
        if self.lifetime.state.get() != State::Replacing(self.identity) {
            return Err(StoreError::Io {
                context: "complete PG allocation lifetime replacement",
                source: std::io::Error::other("replacement no longer owns the local lifetime"),
            });
        }
        self.lifetime.state.set(State::Active(self.identity));
        Ok(())
    }
}

impl Drop for ReplacementLifetime<'_> {
    fn drop(&mut self) {
        if self.lifetime.state.get() == State::Replacing(self.identity) {
            self.lifetime.state.set(State::Retired);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn pg_allocation_lifetime_abandoned_replacement_cannot_restore_old_identity() {
        let lifetime = PgAllocationLifetime::new().unwrap();
        let original = lifetime.active_identity().unwrap();
        let replacement = lifetime.begin_replacement().unwrap();
        assert_eq!(lifetime.active_identity(), None);
        drop(replacement);
        assert_eq!(lifetime.active_identity(), None);
        lifetime.begin_replacement().unwrap().complete().unwrap();
        assert_ne!(lifetime.active_identity().unwrap(), original);
    }

    #[test]
    fn pg_allocation_lifetime_superseded_completion_cannot_resurrect_authority() {
        let lifetime = PgAllocationLifetime::new().unwrap();
        let old = lifetime.begin_replacement().unwrap();
        let replacement = lifetime.begin_replacement().unwrap();
        assert!(old.complete().is_err());
        assert_eq!(lifetime.active_identity(), None);
        replacement.complete().unwrap();
        assert!(lifetime.active_identity().is_some());
    }
}
