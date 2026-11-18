// Copyright The Argmin Authors.
// SPDX-License-Identifier: Apache-2.0

//! Transfer replacement ordering on top of the common SQL teardown owner.
//! No new route authority, callback, format or durability boundary.

use super::super::allocation_lifetime::ReplacementLifetime;
use super::super::metadata_transaction::{with_rollback_error, MetadataTransaction};
use super::{MetadataCommandReplicaState, PgStore, StoreError};

pub(super) struct MetadataTransferTransaction<'pg> {
    // Roll SQL back before discarding replacement custody during unwind.
    transaction: MetadataTransaction<'pg>,
    replacement: ReplacementLifetime<'pg>,
    pg: &'pg PgStore,
    commit_context: &'static str,
}

impl<'pg> MetadataTransferTransaction<'pg> {
    pub(super) fn begin(
        pg: &'pg PgStore,
        begin_context: &'static str,
        commit_context: &'static str,
    ) -> Result<Self, StoreError> {
        pg.require_usable_connection()?;
        // Preserve replacement semantics: even a rejected BEGIN cannot revive
        // the prior allocator. Candidate identity is minted before SQL effects.
        let replacement = pg.allocation_lifetime.begin_replacement()?;
        if !pg.conn.is_autocommit() {
            return Err(StoreError::RouteCapabilitySubjectMismatch {
                operation: "replace metadata transfer state inside caller transaction",
            });
        }
        let transaction = MetadataTransaction::begin(pg, begin_context)?;
        Ok(Self {
            transaction,
            replacement,
            pg,
            commit_context,
        })
    }

    pub(super) fn finish(
        self,
        result: Result<MetadataCommandReplicaState, StoreError>,
    ) -> Result<MetadataCommandReplicaState, StoreError> {
        #[cfg(test)]
        let result = result.and_then(|state| {
            run_hook(self.pg, "before-commit")?;
            Ok(state)
        });
        let state = match result {
            Ok(state) => {
                self.transaction.commit(self.commit_context)?;
                state
            }
            Err(error) => return Err(with_rollback_error(error, self.transaction.rollback())),
        };
        // Local SQL is committed before either refresh or new allocation can
        // succeed. Later errors/unwind must not attempt rollback of that commit.
        #[cfg(test)]
        run_hook(self.pg, "after-commit")?;
        self.pg.mark_metadata_state_digest_clean()?;
        self.replacement.complete()?;
        Ok(state)
    }
}

#[cfg(test)]
type Hook = Box<dyn FnMut(&PgStore, &'static str) -> Result<(), StoreError>>;

#[cfg(test)]
thread_local! {
    static TEST_HOOK: std::cell::RefCell<Option<Hook>> = const { std::cell::RefCell::new(None) };
}

#[cfg(test)]
fn run_hook(pg: &PgStore, stage: &'static str) -> Result<(), StoreError> {
    TEST_HOOK.with(|slot| match slot.borrow_mut().as_mut() {
        Some(hook) => hook(pg, stage),
        None => Ok(()),
    })
}

#[cfg(test)]
#[path = "metadata_transfer_transaction_tests.rs"]
mod tests;
