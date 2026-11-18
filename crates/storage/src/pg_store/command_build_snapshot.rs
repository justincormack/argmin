// Copyright The Argmin Authors.
// SPDX-License-Identifier: Apache-2.0

//! One read-only SQLite view for primary command construction. The caller keeps
//! its PG mutex; no network work belongs inside this scope. This does not certify
//! command acceptance, reserve a generation, or replace effect-boundary fencing.

use super::{MetadataError, PgStore};
use rusqlite::Connection;

fn database(source: rusqlite::Error) -> MetadataError {
    MetadataError::Db {
        context: "primary metadata command build snapshot",
        source: source.into(),
    }
}

struct ReadOnlyMode<'a> {
    connection: &'a Connection,
    original: bool,
    restored: bool,
}

impl ReadOnlyMode<'_> {
    fn restore(&mut self) -> Result<(), MetadataError> {
        self.connection
            .pragma_update(None, "query_only", self.original)
            .map_err(database)?;
        self.restored = true;
        Ok(())
    }
}

impl Drop for ReadOnlyMode<'_> {
    fn drop(&mut self) {
        if !self.restored {
            let _ = self.restore();
        }
    }
}

/// Owns only the transaction opened by this snapshot. Unlike a bare SQLite
/// transaction guard, unconfirmed teardown invalidates the owning PG store.
struct ReadTransaction<'a> {
    pg: &'a PgStore,
    finished: bool,
}

impl ReadTransaction<'_> {
    fn execute_rollback(&self) -> rusqlite::Result<()> {
        #[cfg(test)]
        if self.pg.fail_next_snapshot_rollback.replace(false) {
            return Err(rusqlite::Error::SqliteFailure(
                rusqlite::ffi::Error::new(rusqlite::ffi::SQLITE_IOERR),
                Some("injected read-snapshot rollback failure".into()),
            ));
        }
        self.pg.conn.execute_batch("ROLLBACK")
    }

    fn rollback(&mut self) -> Result<(), MetadataError> {
        let result = self.execute_rollback();
        // No hidden retry on Drop: retirement is sticky until reopen, even if
        // some later caller manages to end the underlying SQLite transaction.
        self.finished = true;
        if !self.pg.conn.is_autocommit() {
            self.pg.retire_connection();
            result.map_err(database)?;
            return Err(MetadataError::InvariantViolation {
                context: "primary metadata command build snapshot",
                reason: "snapshot rollback left a transaction open".into(),
            });
        }
        result.map_err(database)
    }
}

impl Drop for ReadTransaction<'_> {
    fn drop(&mut self) {
        if !self.finished {
            let _ = self.rollback();
        }
    }
}

/// Field order is intentional: rollback precedes restoring writable mode on
/// error/unwind. Successful construction explicitly ends both scopes before
/// exposing its command. Unconfirmed rollback retires the owning store. Never
/// commits, including if a future builder errs.
pub(in crate::node_runtime) struct MetadataCommandBuildSnapshot<'a> {
    transaction: ReadTransaction<'a>,
    mode: ReadOnlyMode<'a>,
}

impl MetadataCommandBuildSnapshot<'_> {
    pub(in crate::node_runtime) fn finish(self) -> Result<(), MetadataError> {
        let Self {
            mut transaction,
            mut mode,
        } = self;
        transaction.rollback()?;
        mode.restore()
    }
}

impl PgStore {
    pub(in crate::node_runtime) fn command_build_snapshot(
        &self,
    ) -> Result<MetadataCommandBuildSnapshot<'_>, MetadataError> {
        self.require_usable_connection()
            .map_err(|error| MetadataError::Db {
                context: "primary metadata command build snapshot",
                source: crate::error::DatabaseError::new(error.to_string()),
            })?;
        // Do not adopt, commit or roll back an enclosing caller transaction.
        if !self.conn.is_autocommit() {
            return Err(MetadataError::InvariantViolation {
                context: "primary metadata command build snapshot",
                reason: "builder requires its own read snapshot".into(),
            });
        }
        let original = self
            .conn
            .pragma_query_value(None, "query_only", |row| row.get(0))
            .map_err(database)?;
        let mode = ReadOnlyMode {
            connection: &self.conn,
            original,
            restored: false,
        };
        self.conn
            .pragma_update(None, "query_only", true)
            .map_err(database)?;
        self.conn
            .execute_batch("BEGIN DEFERRED")
            .map_err(database)?;
        let transaction = ReadTransaction {
            pg: self,
            finished: false,
        };
        Ok(MetadataCommandBuildSnapshot { transaction, mode })
    }
}
