// Copyright The Argmin Authors.
// SPDX-License-Identifier: Apache-2.0

//! Read-only local custody diagnostics. These scalar observations cannot be
//! consumed as deletion/release/retirement authority or as cached validation.

use super::{ContextError, ContextualCommand, Input};
use crate::node_runtime::pg_store::{MetadataCommandStartupDisposition, PgStore};
use crate::StoreError;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(in crate::node_runtime) struct AbandonedPartCustodyObservation {
    pub(in crate::node_runtime) retained_windows: usize,
    pub(in crate::node_runtime) selected_references: usize,
    pub(in crate::node_runtime) additional_finalizers: usize,
}

#[derive(Debug, thiserror::Error)]
#[error(transparent)]
pub(in crate::node_runtime) struct AbandonedPartCustodyError(ContextError);

impl From<StoreError> for AbandonedPartCustodyError {
    fn from(error: StoreError) -> Self {
        Self(error.into())
    }
}

impl ContextualCommand {
    pub(in crate::node_runtime::pg_store) fn inspect_abandoned_part_custody(
        &self,
        pg: &PgStore,
        node_id: u32,
    ) -> Result<AbandonedPartCustodyObservation, AbandonedPartCustodyError> {
        self.inspect_custody(pg, node_id)
            .map_err(AbandonedPartCustodyError)
    }

    fn inspect_custody(
        &self,
        pg: &PgStore,
        node_id: u32,
    ) -> Result<AbandonedPartCustodyObservation, ContextError> {
        pg.require_usable_connection()?;
        if !pg.conn.is_autocommit() {
            return Err(ContextError::Encoding(
                "terminal custody inspection requires its own snapshot",
            ));
        }
        let Input::AbandonedPart(candidate) = &self.input else {
            return Err(ContextError::Encoding(
                "terminal custody inspection requires source-bearing cleanup",
            ));
        };
        let snapshot = pg.command_build_snapshot()?;
        if !matches!(
            self.encode_command()
                .map_err(|_| ContextError::Encoding("encode terminal custody complete command"))?
                .inspect_terminal_in_open_transaction(pg, node_id)?,
            MetadataCommandStartupDisposition::Applied { .. }
        ) {
            return Err(ContextError::Encoding(
                "terminal custody inspection requires exact applied cleanup",
            ));
        }
        let (retained_windows, selected_references, additional_finalizers) =
            super::super::abandonment::inspect_terminal_custody_in_open_transaction(
                pg,
                node_id,
                candidate.bind(&self.topology)?,
            )?;
        snapshot.finish()?;
        Ok(AbandonedPartCustodyObservation {
            retained_windows,
            selected_references,
            additional_finalizers,
        })
    }
}
