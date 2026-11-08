// Copyright The Argmin Authors.
// SPDX-License-Identifier: Apache-2.0

//! One command-bound projection installed with the slot, never a post-commit
//! index patch. The nonempty constructor is owner-local integration plumbing;
//! current command dispatch still selects only the existing empty projection.
//! Mandatory descriptor carriers and frontend custody must activate together.

use super::*;
use crate::node_runtime::pg_store::payload_staging::PendingStagingProtection;
use crate::node_runtime::pg_store::pending_staging_index::PendingStagingIndex;
use crate::PgTopology;

enum StagingProjection<'a> {
    Empty,
    Window {
        index: PendingStagingIndex<'a>,
        topology: &'a PgTopology,
    },
}

pub(super) struct PendingSlotProjection<'a> {
    command: &'a MetadataCommandEnvelope,
    routes: PendingMetadataCommandRouteDependencySidecar,
    staging: StagingProjection<'a>,
}

impl<'a> PendingSlotProjection<'a> {
    pub(super) fn current(command: &'a MetadataCommandEnvelope) -> Self {
        Self {
            command,
            routes: PgStore::pending_metadata_command_route_dependency_sidecar(command),
            staging: StagingProjection::Empty,
        }
    }

    pub(super) fn from_bound(
        protection: &'a PendingStagingProtection<'_>,
        topology: &'a PgTopology,
    ) -> Result<Self, StoreError> {
        let (command, captured_topology, _, _) = protection.index_inputs();
        if topology != captured_topology {
            return Err(StoreError::RouteCapabilitySubjectMismatch {
                operation: "pending-staging-install-topology",
            });
        }
        let mut routes = PgStore::pending_metadata_command_route_dependency_sidecar(command);
        routes.dependencies.extend(protection.route_dependencies());
        routes.dependencies.sort_unstable();
        routes.dependencies.dedup();
        if routes.dependencies.len() > MAX_PENDING_METADATA_COMMAND_ROUTE_DEPENDENCIES {
            return Err(StoreError::RouteCapabilitySubjectMismatch {
                operation: "pending-staging-install-route-count",
            });
        }
        for route in &routes.dependencies {
            route.target.resolve(topology)?;
        }
        routes.checksum = PgStore::pending_metadata_command_route_dependency_checksum(
            command.id(),
            command.checksum_crc64(),
            &routes.dependencies,
        );
        Ok(Self {
            command,
            routes,
            staging: StagingProjection::Window {
                index: PendingStagingIndex::from_bound(protection),
                topology,
            },
        })
    }

    pub(super) fn command(&self) -> &'a MetadataCommandEnvelope {
        self.command
    }

    pub(super) fn install(self, pg: &PgStore) -> Result<(), StoreError> {
        pg.replace_pending_metadata_command_route_dependency_sidecar(&self.routes)?;
        match self.staging {
            StagingProjection::Empty => pg
                .initialize_pending_staging_index(self.command.id(), self.command.checksum_crc64()),
            StagingProjection::Window { index, topology } => {
                index.install_in_open_transaction(pg, topology)
            }
        }
    }

    /// Exact command presence establishes custody even if its indexes are
    /// damaged or differ from the supplied descriptor. Never repair/overwrite
    /// them during an idempotent retry, nor report authoritative noninstallation.
    pub(super) fn require_existing(&self, pg: &PgStore, node_id: u32) -> Result<(), StoreError> {
        let expected = PendingMetadataCommandRouteReferences {
            id: self.command.id(),
            command_checksum: self.command.checksum_crc64(),
            dependencies: self.routes.dependencies.clone(),
        };
        if pg.pending_metadata_command_route_references(node_id)? != Some(expected) {
            return Err(StoreError::RouteCapabilitySubjectMismatch {
                operation: "pending-staging-install-existing-routes",
            });
        }
        match &self.staging {
            StagingProjection::Empty => pg.require_empty_pending_staging_index(self.command),
            StagingProjection::Window { index, .. } => index.require_existing(pg),
        }?;
        // Derive from this exact command under the caller's transaction. Do
        // not accept independently supplied pages as comparison authority.
        let payload = pg.encode_pending_shard_scavenger_reference_pages(self.command)?;
        require_payload_pages(pg, &payload)
    }
}

/// Compare every materialized page, not merely the slot's advertised counts.
/// The caller has matched the exact envelope and holds the pending transaction.
/// No repair or projection extraction is provided through this boundary.
fn require_payload_pages(
    pg: &PgStore,
    expected: &PendingShardScavengerReferencePages,
) -> Result<(), StoreError> {
    let mismatch = || StoreError::RouteCapabilitySubjectMismatch {
        operation: "pending-staging-install-existing-payload-pages",
    };
    let counts = pg.query_row_cached_optional(
        "SELECT placed_segment_reference_count, reclaim_reference_count \
         FROM metadata_command_pending_slot WHERE singleton = 0",
        [],
        "validate pending payload reference counts",
        |row| Ok((row.get::<_, i64>(0)?, row.get::<_, i64>(1)?)),
    )?;
    if counts
        != Some((
            i64::from(expected.placed_reference_count),
            i64::from(expected.reclaim_reference_count),
        ))
    {
        return Err(mismatch());
    }
    for (table, count, pages) in [
        (
            "metadata_command_pending_placed_reference_pages",
            expected.placed_reference_count,
            &expected.placed_pages,
        ),
        (
            "metadata_command_pending_reclaim_reference_pages",
            expected.reclaim_reference_count,
            &expected.reclaim_pages,
        ),
    ] {
        // A corrupt row must not allocate an unbounded BLOB before comparison.
        // Read at most the expected set plus one row to detect extra pages.
        let max_bytes = pages.iter().map(Vec::len).max().unwrap_or(0);
        let limit = pages.len() + 1;
        let sql = format!(
            "SELECT page_index, reference_count, \
             CASE WHEN length(encoded_references) <= ?1 THEN encoded_references ELSE NULL END \
             FROM {table} WHERE singleton = 0 ORDER BY page_index LIMIT ?2"
        );
        let mut statement = pg.conn.prepare(&sql).map_err(|source| StoreError::Db {
            context: "validate pending payload reference pages",
            source: source.into(),
        })?;
        let mut rows = statement
            .query(params![max_bytes, limit])
            .map_err(|source| StoreError::Db {
                context: "validate pending payload reference pages",
                source: source.into(),
            })?;
        let mut index = 0;
        while let Some(row) = rows.next().map_err(|source| StoreError::Db {
            context: "validate pending payload reference pages",
            source: source.into(),
        })? {
            let Some(expected_bytes) = pages.get(index) else {
                return Err(mismatch());
            };
            let expected_count = (count as usize
                - index * usize::from(SHARD_SCAVENGER_REFERENCE_PAGE_LIMIT))
            .min(usize::from(SHARD_SCAVENGER_REFERENCE_PAGE_LIMIT));
            if row.get::<_, usize>(0).ok() != Some(index)
                || row.get::<_, usize>(1).ok() != Some(expected_count)
                || row.get::<_, Option<Vec<u8>>>(2).ok().flatten().as_ref() != Some(expected_bytes)
            {
                return Err(mismatch());
            }
            index += 1;
        }
        if index != pages.len() {
            return Err(mismatch());
        }
    }
    Ok(())
}
