// Copyright The Argmin Authors.
// SPDX-License-Identifier: Apache-2.0

//! Closed adapters for the single pending-install transaction. Selecting the
//! complete adapter never makes the live v13 reader accept v14 or vice versa.

use super::*;
use crate::node_runtime::pg_store::payload_staging::{
    CompletePendingProjection, EncodedContextualCommand,
};

/// Minted only inside the command-log install transaction. A complete
/// projection borrows this scope, not just the longer-lived command or store.
pub(in super::super) struct PendingInstallationTransaction<'pg> {
    pg: &'pg PgStore,
}

impl<'pg> PendingInstallationTransaction<'pg> {
    /// Read-only origin evidence for a lost-reply retry. This never permits a
    /// fresh insert, and does not replace the subsequent exact side-index checks.
    pub(in super::super) fn contains_exact_complete_command(
        &self,
        node: u32,
        command: &EncodedContextualCommand<'_>,
    ) -> Result<bool, StoreError> {
        self.require_store(self.pg)?;
        let slot =
            self.pg
                .pending_metadata_command_slot_with_validator(node, |bytes, checksum, id| {
                    if command.validate_slot_bytes(bytes, checksum, id) {
                        Ok(())
                    } else {
                        Err(StoreError::RouteCapabilitySubjectMismatch {
                            operation: "remote direct installation exact pending origin",
                        })
                    }
                })?;
        Ok(slot.is_some_and(|slot| {
            slot.id == command.id()
                && slot.command_checksum == command.checksum_crc64()
                && slot.command_bytes == command.bytes()
        }))
    }

    pub(super) fn new(pg: &'pg PgStore) -> Self {
        assert!(!pg.conn.is_autocommit(), "pending transaction must be open");
        Self { pg }
    }

    pub(in super::super) fn store(&self) -> &PgStore {
        self.pg
    }

    pub(in super::super) fn require_store(&self, pg: &PgStore) -> Result<(), StoreError> {
        if !std::ptr::eq(pg, self.pg) || pg.conn.is_autocommit() {
            return Err(StoreError::RouteCapabilitySubjectMismatch {
                operation: "complete-pending-install-transaction",
            });
        }
        Ok(())
    }
}

pub(super) enum PendingInstallation<'a> {
    Current(PendingSlotProjection<'a>),
    Complete {
        command: &'a EncodedContextualCommand<'a>,
        topology: &'a crate::PgTopology,
        direct:
            Option<crate::node_runtime::pg_store::payload_staging::DirectInstallationCheck<'a, 'a>>,
    },
}

pub(super) enum PreparedPendingInstallation<'a> {
    Current {
        projection: &'a PendingSlotProjection<'a>,
        pages: PendingShardScavengerReferencePages,
    },
    Complete(Box<CompletePendingProjection<'a>>),
}

impl PendingInstallation<'_> {
    pub(super) fn scope_bucket(&self) -> &BucketName {
        match self {
            Self::Current(p) => p.command().bucket_name(),
            Self::Complete { command, .. } => command.scope_bucket(),
        }
    }

    /// Current callers validate their ordinary/recovery certificate before
    /// selecting this adapter. Complete replacements must additionally retain
    /// the exact context, not merely the same draft payload.
    pub(super) fn require_reissue_of(&self, source: &Self) -> Result<(), StoreError> {
        match (self, source) {
            (Self::Current(_), Self::Current(_)) => Ok(()),
            (
                Self::Complete {
                    command, topology, ..
                },
                Self::Complete {
                    command: source,
                    topology: source_topology,
                    ..
                },
            ) if topology == source_topology => {
                command.require_same_epoch_reissue_of(source, topology)
            }
            _ => Err(StoreError::RouteCapabilitySubjectMismatch {
                operation: "replace complete pending command context",
            }),
        }
    }

    /// A malformed complete slot cannot establish absence of an earlier
    /// replacement. Keep the existing current-format classification unchanged.
    pub(super) fn replacement_observation_error(
        &self,
        error: StoreError,
    ) -> PendingMetadataCommandSlotReplaceError {
        match self {
            Self::Current(_) => PendingMetadataCommandSlotReplaceError::definitive(error),
            Self::Complete { .. } => {
                PendingMetadataCommandSlotReplaceError::may_have_applied(error)
            }
        }
    }

    pub(super) fn id(&self) -> MetadataCommandId {
        match self {
            Self::Current(p) => p.command().id(),
            Self::Complete { command, .. } => command.id(),
        }
    }

    pub(super) fn checksum_crc64(&self) -> u64 {
        match self {
            Self::Current(p) => p.command().checksum_crc64(),
            Self::Complete { command, .. } => command.checksum_crc64(),
        }
    }

    pub(super) fn command_bytes(&self) -> std::borrow::Cow<'_, [u8]> {
        match self {
            Self::Current(p) => p.command().command_bytes().into(),
            Self::Complete { command, .. } => command.bytes().into(),
        }
    }

    pub(super) fn is_bucket_control(&self) -> bool {
        match self {
            Self::Current(p) => p
                .command()
                .payload()
                .is_bucket_control_pending_slot_payload(),
            Self::Complete { command, .. } => command.is_bucket_control(),
        }
    }

    pub(super) fn conflict(&self, node_id: u32) -> StoreError {
        StoreError::MetadataCommandLogConflict {
            node_id,
            pg_id: self.id().pg_id().get(),
            cluster_epoch: self.id().cluster_epoch(),
            log_index: self.id().log_index().get(),
        }
    }

    pub(super) fn prepare<'a>(
        &'a self,
        transaction: &'a PendingInstallationTransaction<'_>,
        node_id: u32,
    ) -> Result<PreparedPendingInstallation<'a>, StoreError> {
        let pg = transaction.store();
        match self {
            Self::Current(projection) => Ok(PreparedPendingInstallation::Current {
                projection,
                pages: pg.encode_pending_shard_scavenger_reference_pages(projection.command())?,
            }),
            Self::Complete {
                command,
                topology,
                direct,
            } => {
                if let Some(proof) = direct {
                    command.require_direct_terminal_installation(
                        transaction,
                        node_id,
                        topology,
                        proof,
                    )?;
                }
                Ok(PreparedPendingInstallation::Complete(Box::new(
                    command.prepare_pending(transaction, node_id, topology)?,
                )))
            }
        }
    }

    pub(super) fn require_existing(
        &self,
        transaction: &PendingInstallationTransaction<'_>,
        node_id: u32,
    ) -> Result<(), StoreError> {
        match self {
            Self::Current(projection) => projection.require_existing(transaction.store(), node_id),
            Self::Complete { .. } => self
                .prepare(transaction, node_id)?
                .require_existing(transaction.store(), node_id),
        }
    }

    pub(super) fn require_authorization(
        &self,
        pg: &PgStore,
        scope: Option<&BucketName>,
    ) -> Result<(), StoreError> {
        match self {
            Self::Current(p) => pg.require_mark_bucket_deleting_install_authorization(
                p.command(),
                scope,
                crate::clock::current_time_millis(),
            ),
            Self::Complete { command, .. } => command.require_install_authorization(pg),
        }
    }

    pub(super) fn slot(
        &self,
        pg: &PgStore,
        node_id: u32,
    ) -> Result<Option<PendingMetadataCommandSlot>, StoreError> {
        let slot = match self {
            Self::Current(_) => {
                pg.pending_metadata_command_slot(node_id, self.id().cluster_epoch())?
            }
            Self::Complete { command, .. } => {
                pg.pending_metadata_command_slot_with_validator(node_id, |bytes, checksum, id| {
                    if command.validate_slot_bytes(bytes, checksum, id) {
                        Ok(())
                    } else {
                        Err(StoreError::MetadataCommandLogConflict {
                            node_id,
                            pg_id: id.pg_id().get(),
                            cluster_epoch: id.cluster_epoch(),
                            log_index: id.log_index().get(),
                        })
                    }
                })?
            }
        };
        if let Some(slot) = &slot {
            if slot.id.cluster_epoch() != self.id().cluster_epoch() {
                return Err(StoreError::StaleMetadataOperation {
                    pg_id: pg.pg_id(),
                    operation_epoch: self.id().cluster_epoch(),
                    current_epoch: slot.id.cluster_epoch(),
                });
            }
        }
        Ok(slot)
    }
}

impl PreparedPendingInstallation<'_> {
    pub(super) fn pages(&self) -> &PendingShardScavengerReferencePages {
        match self {
            Self::Current { pages, .. } => pages,
            Self::Complete(p) => p.pages(),
        }
    }

    pub(super) fn install(&self, pg: &PgStore) -> Result<(), StoreError> {
        match self {
            Self::Current { projection, .. } => projection.install(pg),
            Self::Complete(p) => {
                p.require_store(pg)?;
                let routes = complete_routes(p);
                pg.replace_pending_metadata_command_route_dependency_sidecar(&routes)?;
                pg.install_complete_pending_staging_index(p)
            }
        }
    }

    pub(super) fn require_existing(&self, pg: &PgStore, node_id: u32) -> Result<(), StoreError> {
        match self {
            Self::Current { projection, .. } => projection.require_existing(pg, node_id),
            Self::Complete(p) => p.require_existing(pg, node_id),
        }
    }
}

impl PgStore {
    pub(in super::super) fn require_complete_pending_indexes(
        &self,
        p: &crate::node_runtime::pg_store::payload_staging::CompletePendingIndexes<'_>,
        node_id: u32,
    ) -> Result<(), StoreError> {
        p.require_store(self)?;
        // Scope is a protection index too: bucket-drain clearing uses
        // it without decoding command_bytes. Exact bytes cannot make
        // a missing/crossed scope safe, nor prove noninstallation.
        let command = p.command();
        let matches: bool = self.query_row_cached(
            "SELECT EXISTS(SELECT 1 FROM metadata_command_pending_slot \
             WHERE singleton = 0 AND cluster_epoch = ?1 AND pg_id = ?2 \
             AND log_index = ?3 AND command_checksum = ?4 \
             AND command_bytes = ?5 AND scope_bucket = ?6)",
            params![
                command.id().cluster_epoch().get() as i64,
                command.id().pg_id().get(),
                command.id().log_index().get() as i64,
                command.checksum_crc64() as i64,
                command.bytes(),
                command.scope_bucket(),
            ],
            "bind complete pending indexes to exact slot and scope",
            |row| row.get(0),
        )?;
        if !matches {
            return Err(StoreError::RouteCapabilitySubjectMismatch {
                operation: "pending-staging-install-existing-scope",
            });
        }
        let expected = PendingMetadataCommandRouteReferences {
            id: p.command().id(),
            command_checksum: p.command().checksum_crc64(),
            command_sha256: p.command().command_sha256(),
            dependencies: p.routes().to_vec(),
        };
        if self.pending_metadata_command_route_references(node_id)? != Some(expected) {
            return Err(StoreError::RouteCapabilitySubjectMismatch {
                operation: "pending-staging-install-existing-routes",
            });
        }
        self.require_complete_pending_staging_index(p)?;
        pending_slot_projection::require_payload_pages(self, p.pages())
    }
}

fn complete_routes(
    p: &CompletePendingProjection<'_>,
) -> PendingMetadataCommandRouteDependencySidecar {
    PendingMetadataCommandRouteDependencySidecar {
        command_checksum: p.command().checksum_crc64(),
        command_sha256: p.command().command_sha256(),
        dependencies: p.routes().to_vec(),
        checksum: PgStore::pending_metadata_command_route_dependency_checksum(
            p.command().id(),
            p.command().checksum_crc64(),
            p.routes(),
            &p.command().command_sha256(),
        ),
    }
}
