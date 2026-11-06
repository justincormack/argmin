// Copyright The Argmin Authors.
// SPDX-License-Identifier: Apache-2.0

//! Protection-only discovery for materialized staging responsibility.
//!
//! A window can have no selected manifest (or no physical writes at all). Its
//! bounds are not actual stored lengths, and it has no committed CRC/witness.
//! Consequently this projection must never become a placed/readable backfill
//! reference. It neither authorizes deletion nor proves absence when a scan
//! completes. The unpaged cleanup-reference scan uses a conservative identity-
//! only projection; paged audit carries exact original participants separately.

use std::num::NonZeroU16;
use std::sync::Arc;

use rusqlite::{params, Connection, OptionalExtension};

use super::{sql, *};
use crate::{types::SHARD_SCAVENGER_REFERENCE_PAGE_LIMIT, ShardKey};

fn topology_digest(topology: &PgTopology) -> [u8; 32] {
    let mut bytes = b"staging-protection-cursor-topology-v1".to_vec();
    topology
        .for_each_pg::<std::convert::Infallible>(|pg| {
            bytes.extend_from_slice(&pg.to_le_bytes());
            Ok(())
        })
        .expect("infallible topology iteration");
    checksum::compute_checksum(checksum::ChecksumAlgorithm::Sha256, &bytes)
        .bytes()
        .try_into()
        .expect("SHA-256 length")
}

pub(super) fn extend_scavenger_page(
    conn: &Connection,
    pg_id: PgId,
    topology: &PgTopology,
    after: Option<&crate::types::ShardScavengerReferenceCursor>,
    limit: NonZeroU16,
    items: &mut Vec<crate::types::ShardScavengerReferencePageItem>,
) -> Result<bool, InventoryError> {
    use crate::types::{
        ShardScavengerPayloadReference, ShardScavengerProtectedShardSetReference,
        ShardScavengerReferenceCursor as Cursor, ShardScavengerReferencePageItem,
        ShardScavengerStagingProtection,
    };
    let digest = topology_digest(topology);
    let after = match after {
        Some(Cursor::StagingWindow {
            pg_id: cursor_pg,
            topology_digest: cursor_topology,
            attempt,
            introduction_epoch,
            introduction_index,
            window_index,
        }) => {
            if *cursor_pg != pg_id || *cursor_topology != digest {
                return Err(InventoryError::CursorDomain);
            }
            Some(ProtectionCursor {
                pg_id,
                topology: Arc::new(topology.clone()),
                owner: StagingOwnerId {
                    attempt: attempt.clone(),
                    introduction: MetadataCommandId::new(
                        *introduction_epoch,
                        pg_id,
                        crate::metadata_command::MetadataCommandLogIndex::new(*introduction_index)
                            .ok_or(InventoryError::CursorDomain)?,
                    ),
                },
                window_index: *window_index,
            })
        }
        _ => None,
    };
    let page = protection_page(conn, pg_id, topology, after.as_ref(), limit)?;
    for protected in page.windows {
        let window = protected.window;
        let owner = protected.cursor.owner;
        items.push(ShardScavengerReferencePageItem {
            cursor: Cursor::StagingWindow {
                pg_id,
                topology_digest: digest,
                attempt: owner.attempt,
                introduction_epoch: owner.introduction.cluster_epoch(),
                introduction_index: owner.introduction.log_index().get(),
                window_index: window.index,
            },
            reference: ShardScavengerPayloadReference::StagingProtection(
                ShardScavengerStagingProtection {
                    identity: ShardScavengerProtectedShardSetReference {
                        data_pg_id: window.participants.data_pg.get(),
                        okh: window.segment_hash,
                        generation_id: window.start,
                        ec: window.geometry.ec,
                    },
                    placement_epoch: window.participants.placement_epoch,
                    file_nodes: window.participants.files,
                    acknowledgement_primary: window.participants.acknowledgement_primary,
                },
            ),
        });
    }
    Ok(page.complete)
}

/// Process-local resume hint, not durable evidence or a cleanup capability.
/// Preserve the introduction identity so replacement of the same attempt name
/// cannot silently reuse a cursor and skip that replacement's earlier windows.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct ProtectionCursor {
    pg_id: PgId,
    topology: Arc<PgTopology>,
    owner: StagingOwnerId,
    window_index: u32,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct ProtectedWindow {
    pub(super) cursor: ProtectionCursor,
    window: StagingWindow,
    // Kept separate from the window: selected bindings do not cover unused
    // scope, and losing a binding does not remove its protection obligation.
    pub(super) selected: Option<StagingReference>,
}

impl ProtectedWindow {
    /// Lossy only in the conservative direction: retain every location of this
    /// physical identity. Do not infer actual stored geometry or readability
    /// from the window's bounds or its optional selected manifest binding.
    fn protection_reference(&self) -> crate::types::ShardScavengerPayloadReference {
        crate::types::ShardScavengerPayloadReference::ProtectionOnly(
            crate::types::ShardScavengerProtectedShardSetReference {
                data_pg_id: self.window.participants.data_pg.get(),
                okh: self.window.segment_hash,
                generation_id: self.window.start,
                ec: self.window.geometry.ec,
            },
        )
    }

    fn matches_key(&self, data_pg: PgId, epoch: ClusterEpoch, key: &ShardKey) -> bool {
        data_pg == self.window.participants.data_pg
            && epoch == self.window.participants.placement_epoch
            // Central validation currently requires the one-slot [start,end)
            // interval; do not project its bound as an actual payload length.
            && *key
                == ShardKey::new(
                    &self.window.segment_hash,
                    self.window.start.get(),
                    key.shard_index().get(),
                )
            && usize::from(key.shard_index().get()) < self.window.participants.files.len()
    }

    /// Exact original file participant, not a recomputed current placement.
    pub(super) fn protects_file(
        &self,
        data_pg: PgId,
        epoch: ClusterEpoch,
        node: NodeId,
        key: &ShardKey,
    ) -> bool {
        self.matches_key(data_pg, epoch, key)
            && self.window.participants.files[usize::from(key.shard_index().get())] == node
    }

    /// The acknowledgement primary may be a spare holding no shard files.
    pub(super) fn protects_acknowledgement(
        &self,
        data_pg: PgId,
        epoch: ClusterEpoch,
        node: NodeId,
        key: &ShardKey,
    ) -> bool {
        self.matches_key(data_pg, epoch, key)
            && self.window.participants.acknowledgement_primary == node
    }
}

#[derive(Debug)]
pub(super) struct ProtectionPage {
    pub(super) windows: Vec<ProtectedWindow>,
    pub(super) complete: bool,
}

#[derive(Debug, thiserror::Error)]
pub(super) enum InventoryError {
    #[error("staging protection cursor belongs to another PG or topology")]
    CursorDomain,
    #[error("staging protection cursor's introduction has been replaced; restart the scan")]
    CursorReplaced,
    #[error(transparent)]
    Persistence(#[from] sql::PersistenceError),
    #[error(transparent)]
    Invariant(#[from] StagingInvariant),
    #[error(transparent)]
    Sql(#[from] rusqlite::Error),
}

/// The owning PG lock and caller's read transaction cover owner selection and
/// every child read. Enumerate owners, not selected references/windows joined to
/// sessions: even a malformed owner with all windows missing must fail closed.
/// Work is bounded by at most `limit + 1` owners, each with the centrally bounded
/// window inventory. No timestamp, current route or physical I/O is consulted.
pub(super) fn protection_page(
    conn: &Connection,
    pg_id: PgId,
    topology: &PgTopology,
    after: Option<&ProtectionCursor>,
    limit: NonZeroU16,
) -> Result<ProtectionPage, InventoryError> {
    if conn.is_autocommit() {
        return Err(sql::PersistenceError::TransactionRequired.into());
    }
    if after.is_some_and(|cursor| cursor.pg_id != pg_id || cursor.topology.as_ref() != topology) {
        return Err(InventoryError::CursorDomain);
    }
    let limit = usize::from(limit.get().min(SHARD_SCAVENGER_REFERENCE_PAGE_LIMIT));
    // Include the cursor's owner to discover subsequently appended windows.
    // One extra owner permits resuming after that owner's last window without
    // returning an empty, incomplete page with no way to advance the cursor.
    let mut statement = conn.prepare_cached(
        "SELECT attempt FROM payload_staging_owners \
         WHERE attempt >= ?1 ORDER BY attempt LIMIT ?2",
    )?;
    let attempts = statement
        .query_map(
            params![
                after.map_or("", |cursor| cursor.owner.attempt.as_str()),
                limit + 1
            ],
            |row| row.get::<_, SessionId>(0),
        )?
        .collect::<rusqlite::Result<Vec<_>>>()?;
    let mut windows = Vec::with_capacity(limit);
    let topology_identity = Arc::new(topology.clone());
    for attempt in attempts {
        let owner = sql::load(conn, pg_id, &attempt)?
            .ok_or(sql::PersistenceError::Row("inventory owner disappeared"))?;
        owner.validate(topology)?;
        let high_water = conn
            .query_row(
                "SELECT max_committed_generation FROM object_write_counters \
                 WHERE bucket = ?1 AND key = ?2",
                params![owner.subject.bucket, owner.subject.key],
                |row| row.get::<_, Option<u64>>(0),
            )
            .optional()?
            .flatten();
        owner.subject.require_generation_high_water(high_water)?;
        let owner_after = after.filter(|cursor| cursor.owner.attempt == attempt);
        if owner_after.is_some_and(|cursor| cursor.owner != owner.subject.id) {
            return Err(InventoryError::CursorReplaced);
        }
        for window in &owner.windows {
            if owner_after.is_some_and(|cursor| window.index <= cursor.window_index) {
                continue;
            }
            if windows.len() == limit {
                return Ok(ProtectionPage {
                    windows,
                    complete: false,
                });
            }
            windows.push(ProtectedWindow {
                cursor: ProtectionCursor {
                    pg_id,
                    topology: Arc::clone(&topology_identity),
                    owner: owner.subject.id.clone(),
                    window_index: window.index,
                },
                window: window.clone(),
                selected: owner
                    .references
                    .iter()
                    .find(|reference| reference.window_index == window.index)
                    .cloned(),
            });
        }
    }
    // If the owner-query bound is exhausted exactly, the next page may be empty
    // and complete. This is a bounded scan hint, not an atomic global snapshot.
    Ok(ProtectionPage {
        complete: windows.len() < limit,
        windows,
    })
}

/// The unpaged API already returns all live/pending/reclaim references. Add
/// every retained window from the same read transaction, including unused or
/// expired scope. A later error discards the caller's whole response; no prefix
/// is returned as a complete inventory.
pub(super) fn extend_protection_references(
    conn: &Connection,
    pg_id: PgId,
    topology: &PgTopology,
    references: &mut Vec<crate::types::ShardScavengerPayloadReference>,
) -> Result<(), InventoryError> {
    let mut after = None;
    loop {
        let page = protection_page(
            conn,
            pg_id,
            topology,
            after.as_ref(),
            NonZeroU16::new(SHARD_SCAVENGER_REFERENCE_PAGE_LIMIT).expect("nonzero page limit"),
        )?;
        for window in page.windows {
            references.push(window.protection_reference());
            after = Some(window.cursor);
        }
        if page.complete {
            return Ok(());
        }
    }
}
