// Copyright The Argmin Authors.
// SPDX-License-Identifier: Apache-2.0

//! Store-owned transaction teardown, not command or validation authority.
//! Storage implementation helpers and closed command operations use this primitive. It
//! exposes neither the connection nor a callback, and cannot mark a PG valid.
//! Raw SQL callers have not yet all migrated to this boundary.

use super::{PgStore, StoreError};
use std::cell::Cell;

#[derive(Default)]
pub(super) struct Scopes {
    next: Cell<u64>,
    current: Cell<Option<u64>>,
}

#[cfg(test)]
impl Scopes {
    pub(super) fn has_active_scope(&self) -> bool {
        self.current.get().is_some()
    }
}

enum Scope {
    Outer,
    Savepoint(String),
}

pub(super) struct MetadataTransaction<'pg> {
    pg: &'pg PgStore,
    scope: Scope,
    id: u64,
    parent: Option<u64>,
    finished: bool,
}

fn database(context: &'static str, source: rusqlite::Error) -> StoreError {
    StoreError::Db {
        context,
        source: source.into(),
    }
}

impl<'pg> MetadataTransaction<'pg> {
    /// Operations that promise a standalone commit must not silently become
    /// savepoints inside a caller's transaction.
    pub(super) fn begin_outer(pg: &'pg PgStore, context: &'static str) -> Result<Self, StoreError> {
        pg.require_usable_connection()?;
        if !pg.conn.is_autocommit() {
            return Err(StoreError::RouteCapabilitySubjectMismatch {
                operation: "standalone metadata transaction requires autocommit",
            });
        }
        Self::begin(pg, context)
    }

    pub(super) fn begin(pg: &'pg PgStore, context: &'static str) -> Result<Self, StoreError> {
        pg.require_usable_connection()?;
        let parent = pg.metadata_transaction_scopes.current.get();
        let outer = pg.conn.is_autocommit();
        if outer && parent.is_some() {
            pg.retire_connection();
            return Err(StoreError::RouteCapabilitySubjectMismatch {
                operation: "metadata transaction ended outside its owner",
            });
        }
        let id = pg
            .metadata_transaction_scopes
            .next
            .get()
            .checked_add(1)
            .ok_or(StoreError::RouteCapabilitySubjectMismatch {
                operation: "metadata transaction scope identity exhausted",
            })?;
        let scope = if outer {
            pg.conn
                .execute_batch("BEGIN IMMEDIATE")
                .map_err(|error| database(context, error))?;
            Scope::Outer
        } else {
            // Unique, storage-generated names: never accept caller SQL or reuse
            // a parent's savepoint name. A child release is NOT an outer commit.
            let name = format!("argmin_metadata_scope_{id}");
            pg.conn
                .execute_batch(&format!("SAVEPOINT {name}"))
                .map_err(|error| database(context, error))?;
            Scope::Savepoint(name)
        };
        pg.metadata_transaction_scopes.next.set(id);
        pg.metadata_transaction_scopes.current.set(Some(id));
        Ok(Self {
            pg,
            scope,
            id,
            parent,
            finished: false,
        })
    }

    fn require_current(&self) -> Result<(), StoreError> {
        self.pg.require_usable_connection()?;
        if self.pg.metadata_transaction_scopes.current.get() != Some(self.id)
            || self.pg.conn.is_autocommit()
        {
            // Out-of-order release could commit another scope's changes.
            // Do not issue COMMIT/RELEASE or attempt to repair its ownership.
            self.pg.retire_connection();
            return Err(StoreError::RouteCapabilitySubjectMismatch {
                operation: "metadata transaction scope ownership changed",
            });
        }
        Ok(())
    }

    fn finish(&mut self) {
        self.finished = true;
        if self.pg.metadata_transaction_scopes.current.get() == Some(self.id) {
            self.pg.metadata_transaction_scopes.current.set(self.parent);
        }
    }

    fn rollback_inner(&mut self) -> Result<(), StoreError> {
        let result = self.require_current().and_then(|()| {
            let sql = match &self.scope {
                Scope::Outer => "ROLLBACK".to_owned(),
                Scope::Savepoint(name) => format!("ROLLBACK TO {name}; RELEASE {name}"),
            };
            self.pg
                .execute_metadata_command_rollback(&sql)
                .map_err(|error| database("rollback owned metadata transaction", error))?;
            let expected_autocommit = matches!(self.scope, Scope::Outer);
            if self.pg.conn.is_autocommit() != expected_autocommit {
                return Err(StoreError::RouteCapabilitySubjectMismatch {
                    operation: "metadata rollback changed enclosing transaction",
                });
            }
            Ok(())
        });
        self.pg.invalidate_clean_metadata_digest_revision();
        if result.is_err() {
            // Sticky even if some raw caller later succeeds in ending SQLite's
            // transaction. No hidden retry in Drop and no caller can commit via
            // another managed scope after this child failed to roll back.
            self.pg.retire_connection();
        }
        self.finish();
        result
    }

    pub(super) fn rollback(mut self) -> Result<(), StoreError> {
        self.rollback_inner()
    }

    pub(super) fn commit(mut self, context: &'static str) -> Result<(), StoreError> {
        self.require_current()?;
        let result = (|| {
            #[cfg(test)]
            if matches!(self.scope, Scope::Outer)
                && self
                    .pg
                    .fail_next_metadata_txn_commit
                    .swap(false, std::sync::atomic::Ordering::Relaxed)
            {
                return Err(StoreError::Db {
                    context,
                    source: crate::error::DatabaseError::new(
                        "injected metadata transaction commit failure",
                    ),
                });
            }
            let sql = match &self.scope {
                Scope::Outer => "COMMIT".to_owned(),
                Scope::Savepoint(name) => format!("RELEASE {name}"),
            };
            self.pg
                .conn
                .execute_batch(&sql)
                .map_err(|error| database(context, error))?;
            if self.pg.conn.is_autocommit() != matches!(self.scope, Scope::Outer) {
                self.pg.retire_connection();
                return Err(StoreError::RouteCapabilitySubjectMismatch {
                    operation: "metadata commit changed enclosing transaction",
                });
            }
            Ok(())
        })();
        match result {
            Ok(()) => {
                self.finish();
                Ok(())
            }
            Err(cause) => {
                // A failed COMMIT is still an ambiguous operation outcome even
                // if local rollback succeeds; callers keep their custody rules.
                let rollback = self.rollback_inner();
                Err(with_rollback_error(cause, rollback))
            }
        }
    }
}

/// Retain both the operation cause and teardown diagnostic. Success here says
/// only that rollback was confirmed, never that an earlier send was unaccepted.
pub(super) fn with_rollback_error(
    cause: StoreError,
    rollback: Result<(), StoreError>,
) -> StoreError {
    match rollback {
        Ok(()) => cause,
        Err(rollback) => StoreError::Db {
            context: "metadata operation and rollback failed",
            source: crate::error::DatabaseError::new(format!(
                "operation: {cause}; rollback failed: {rollback}"
            )),
        },
    }
}

impl Drop for MetadataTransaction<'_> {
    fn drop(&mut self) {
        if !self.finished {
            let _ = self.rollback_inner();
        }
    }
}

#[cfg(test)]
#[path = "metadata_transaction/tests.rs"]
mod tests;
