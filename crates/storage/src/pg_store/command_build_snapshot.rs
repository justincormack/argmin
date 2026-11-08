// Copyright The Argmin Authors.
// SPDX-License-Identifier: Apache-2.0

//! One read-only SQLite view for primary command construction. The caller keeps
//! its PG mutex; no network work belongs inside this scope. This does not certify
//! command acceptance, reserve a generation, or replace effect-boundary fencing.

use super::{MetadataError, PgStore};
use rusqlite::{Connection, Transaction, TransactionBehavior};

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

/// Field order is intentional: rollback precedes restoring writable mode on
/// error/unwind. Successful construction explicitly ends both scopes before
/// exposing its command. Never commits, including if a future builder errs.
pub(in crate::node_runtime) struct MetadataCommandBuildSnapshot<'a> {
    transaction: Transaction<'a>,
    mode: ReadOnlyMode<'a>,
}

impl MetadataCommandBuildSnapshot<'_> {
    pub(in crate::node_runtime) fn finish(self) -> Result<(), MetadataError> {
        let Self {
            transaction,
            mut mode,
        } = self;
        transaction.rollback().map_err(database)?;
        mode.restore()
    }
}

impl PgStore {
    pub(in crate::node_runtime) fn command_build_snapshot(
        &self,
    ) -> Result<MetadataCommandBuildSnapshot<'_>, MetadataError> {
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
        let transaction = Transaction::new_unchecked(&self.conn, TransactionBehavior::Deferred)
            .map_err(database)?;
        Ok(MetadataCommandBuildSnapshot { transaction, mode })
    }
}
