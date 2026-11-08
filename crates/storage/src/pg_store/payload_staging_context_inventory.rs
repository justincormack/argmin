// Copyright The Argmin Authors.
// SPDX-License-Identifier: Apache-2.0

//! Complete-context protection inventory, not installation or physical authority.
//! Current v13 slots cannot authenticate the context; the coordinated carrier
//! switch must bind these indexes to the complete command in one transaction.

use super::{ContextError, ContextualCommand, Input};
#[cfg(test)]
use crate::node_runtime::pg_store::PendingShardScavengerReferencePages;
use crate::node_runtime::pg_store::PgStore;
use crate::types::ShardScavengerPayloadReference;
use crate::{PgTopology, StoreError};

#[path = "payload_staging_context_retention.rs"]
mod retention;

pub(in super::super::super) use retention::ContextPendingRetention;

pub(in super::super::super) struct ContextPendingInventory {
    retention: ContextPendingRetention,
    payload: Vec<ShardScavengerPayloadReference>,
}

/// Freely copyable diagnostics, not reference identities or encoded indexes.
/// These observations may outlive preparation without retaining installable
/// inventory. Installation must rederive that inventory in its own transaction.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(in super::super::super) struct PendingInventoryObservation {
    pub(in super::super::super) payload_reference_count: usize,
    pub(in super::super::super) route_dependency_count: usize,
    pub(in super::super::super) has_future_window: bool,
}

impl ContextPendingInventory {
    pub(super) fn observation(&self) -> PendingInventoryObservation {
        PendingInventoryObservation {
            payload_reference_count: self.payload.len(),
            route_dependency_count: self.retention.route_dependency_count(),
            has_future_window: self.retention.has_future_window(),
        }
    }
}

// Exact physical reference/route observations and owned encoding belong only
// to owner-local component/format tests, never the production preparation API.
#[cfg(test)]
impl ContextPendingInventory {
    pub(in super::super::super) fn retention(&self) -> &ContextPendingRetention {
        &self.retention
    }

    pub(in super::super::super) fn payload(&self) -> &[ShardScavengerPayloadReference] {
        &self.payload
    }

    /// Same compact page grammar as current slots. Future/unselected windows
    /// stay in their separate protection index, never masquerading as readable
    /// placed payload. This method has no SQL effects or slot installer.
    pub(in super::super::super) fn encode_reference_pages(
        &self,
    ) -> Result<PendingShardScavengerReferencePages, StoreError> {
        PgStore::encode_pending_payload_reference_pages(&self.payload)
    }
}

pub(super) fn derive(
    context: &ContextualCommand,
    pg: &PgStore,
    topology: &PgTopology,
) -> Result<ContextPendingInventory, ContextError> {
    pg.require_usable_connection()?;
    let retention = retention::derive(context, topology)?;
    if context.id().pg_id().get() != pg.pg_id() {
        return Err(ContextError::CommandPg);
    }
    if pg.conn.is_autocommit() {
        return Err(ContextError::InventoryTransaction);
    }
    let payload = match &context.input {
        Input::AbandonedPart(candidate) => {
            candidate.bind(topology)?.pending_payload_references()?
        }
        Input::Ordinary(command) => pg.pending_command_payload_references(command.payload())?,
        Input::Described(candidate) => {
            pg.pending_command_payload_references(candidate.command().payload())?
        }
    };
    for reference in &payload {
        let data_pg = match reference {
            ShardScavengerPayloadReference::Placed(reference) => reference.data_pg_id,
            ShardScavengerPayloadReference::ProtectionOnly(reference) => reference.data_pg_id,
            ShardScavengerPayloadReference::StagingProtection(_) => {
                return Err(incomplete("future scope cannot be selected payload").into());
            }
        };
        if !topology.contains_pg(crate::PgId::new(data_pg)) {
            return Err(incomplete("payload data PG is outside configured topology").into());
        }
    }
    Ok(ContextPendingInventory { retention, payload })
}

#[cfg(test)]
pub(super) fn derive_retention_for_test(
    context: &ContextualCommand,
    topology: &PgTopology,
) -> Result<ContextPendingRetention, ContextError> {
    retention::derive(context, topology)
}

fn incomplete(reason: &str) -> StoreError {
    StoreError::ShardScavengerScanIncomplete {
        context: "derive pending command payload inventory",
        errors: reason.to_owned(),
    }
}
