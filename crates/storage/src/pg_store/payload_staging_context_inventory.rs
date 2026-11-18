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
use rusqlite::OptionalExtension;

#[path = "payload_staging_context_retention.rs"]
mod retention;

pub(in super::super::super) use retention::ContextPendingRetention;

pub(in super::super::super) struct ContextPendingInventory {
    pub(super) retention: ContextPendingRetention,
    pub(super) payload: Vec<ShardScavengerPayloadReference>,
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
    derive_inner(context, pg, topology, false)
}

// Startup inspection cannot require a live session after terminal application.
// Only this read-only path uses the retained owner's immutable window geometry;
// fresh installation keeps its existing live-session requirements.
pub(super) fn derive_retained(
    context: &ContextualCommand,
    pg: &PgStore,
    topology: &PgTopology,
) -> Result<ContextPendingInventory, ContextError> {
    derive_inner(context, pg, topology, true)
}

fn derive_inner(
    context: &ContextualCommand,
    pg: &PgStore,
    topology: &PgTopology,
    retained: bool,
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
        Input::Ordinary(command) if retained => retained_payload(pg, topology, command.payload())?,
        Input::Described(candidate) if retained => {
            let bound = candidate.bind(topology)?;
            if let crate::metadata_command::MetadataCommandPayload::AppendStreamSegment(_) =
                candidate.command().payload()
            {
                let protection = bound.pending_protection();
                let (_, _, subject, _) = protection.index_inputs();
                let owner = load_retained_owner(
                    pg,
                    topology,
                    &subject.bucket,
                    &subject.key,
                    &subject.id.attempt,
                )?;
                if owner.subject != *subject {
                    return Err(
                        incomplete("retained append owner differs from captured subject").into(),
                    );
                }
            }
            retained_payload(pg, topology, candidate.command().payload())?
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

fn load_retained_owner(
    pg: &PgStore,
    topology: &PgTopology,
    bucket: &crate::BucketName,
    key: &crate::ObjectKey,
    session: &crate::SessionId,
) -> Result<super::super::StagingOwnerRecord, StoreError> {
    let owner = super::super::sql::load(&pg.conn, crate::PgId::new(pg.pg_id()), session)
        .map_err(|e| incomplete(&e.to_string()))?
        .ok_or_else(|| incomplete("missing retained owner for pending stream payload"))?;
    owner
        .validate(topology)
        .map_err(|e| incomplete(&e.to_string()))?;
    if owner.subject.bucket != *bucket
        || owner.subject.key != *key
        || owner.subject.id.attempt != *session
        || matches!(owner.subject.target, super::super::StagingTarget::DirectPut)
    {
        return Err(incomplete(
            "crossed retained owner for pending stream payload",
        ));
    }
    let high_water = pg.conn.query_row(
        "SELECT max_committed_generation FROM object_write_counters WHERE bucket = ?1 AND key = ?2",
        rusqlite::params![bucket, key], |row| row.get::<_, Option<u64>>(0),
    ).optional().map_err(|e| incomplete(&e.to_string()))?.flatten();
    owner
        .subject
        .require_generation_high_water(high_water)
        .map_err(|e| incomplete(&e.to_string()))?;
    Ok(owner)
}

fn retained_payload(
    pg: &PgStore,
    topology: &PgTopology,
    payload: &crate::metadata_command::MetadataCommandPayload,
) -> Result<Vec<ShardScavengerPayloadReference>, StoreError> {
    use crate::metadata_command::MetadataCommandPayload;
    let (bucket, key, session, segments) = match payload {
        MetadataCommandPayload::AppendStreamSegment(append) => (
            &append.bucket,
            &append.key,
            &append.segment.session_id,
            std::slice::from_ref(&append.segment),
        ),
        MetadataCommandPayload::AbortStreamUpload(abort) => (
            &abort.bucket,
            &abort.key,
            &abort.session_id,
            abort.staged_segments.as_slice(),
        ),
        _ => return pg.pending_command_payload_references(payload),
    };
    if segments.is_empty() {
        return Ok(Vec::new());
    }
    let owner = load_retained_owner(pg, topology, bucket, key, session)?;
    segments
        .iter()
        .map(|segment| {
            let selected = owner
                .selected_stream_reference(segment)
                .map_err(|e| incomplete(&e.to_string()))?;
            Ok(ShardScavengerPayloadReference::Placed(
                crate::types::ShardScavengerPlacedShardSetReference {
                    data_pg_id: segment.data_pg_id,
                    okh: segment.segment_okh,
                    generation_id: segment.segment_vid,
                    placement_cluster_epoch: segment.placement_cluster_epoch,
                    stored_size: selected.stored_size,
                    crc64: segment.segment_crc64,
                    ec: crate::EcShape {
                        k: segment.ec_k,
                        m: segment.ec_m,
                    },
                    shard_integrity: segment.shard_integrity.clone(),
                },
            ))
        })
        .collect()
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
