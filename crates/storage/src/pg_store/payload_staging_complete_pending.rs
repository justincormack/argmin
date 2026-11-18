// Copyright The Argmin Authors.
// SPDX-License-Identifier: Apache-2.0

//! Transaction-local installation projection of the complete carrier. No
//! constructor accepts independent indexes, source evidence or command bytes.
//! The owning submission never exports this view or its encoded command.

use super::{ContextError, EncodedContextualCommand, Input};
use crate::metadata_command::MetadataCommandId;
use crate::node_runtime::pg_store::{
    command_log::{PendingInstallationTransaction, PendingMetadataCommandRouteDependency},
    payload_staging::{StagingSubject, StagingWindow},
    PendingShardScavengerReferencePages, PgStore,
};
use crate::{BucketName, PgTopology, StoreError};

pub(in super::super::super::super) struct CompletePendingProjection<'a> {
    transaction: &'a PendingInstallationTransaction<'a>,
    indexes: CompletePendingIndexes<'a>,
}

// Read-only comparison inputs, never installation authority. Constructors stay
// below the complete context; the public operation returns no owned projection.
pub(in super::super::super::super) struct CompletePendingIndexes<'a> {
    pg: &'a PgStore,
    command: &'a EncodedContextualCommand<'a>,
    topology: &'a PgTopology,
    routes: Vec<PendingMetadataCommandRouteDependency>,
    window: Option<(StagingSubject, StagingWindow)>,
    pages: PendingShardScavengerReferencePages,
}

impl super::super::ContextualCommand {
    pub(in super::super::super::super) fn scope_bucket(&self) -> &BucketName {
        match &self.input {
            Input::Ordinary(command) => command.bucket_name(),
            Input::Described(candidate) => candidate.command().bucket_name(),
            Input::AbandonedPart(candidate) => candidate.command().bucket_name(),
        }
    }
}

impl EncodedContextualCommand<'_> {
    pub(in super::super::super::super) fn require_same_epoch_reissue_of(
        &self,
        source: &Self,
        topology: &PgTopology,
    ) -> Result<(), StoreError> {
        if self
            .context
            .pending_match(source.context, topology)
            .is_ok_and(|matched| matched == super::super::PendingContextMatch::SameEpochReissue)
        {
            Ok(())
        } else {
            Err(StoreError::RouteCapabilitySubjectMismatch {
                operation: "replace complete pending command context",
            })
        }
    }

    pub(in super::super::super::super) fn scope_bucket(&self) -> &BucketName {
        self.context.scope_bucket()
    }

    pub(in super::super::super::super) fn is_bucket_control(&self) -> bool {
        self.draft()
            .payload()
            .is_bucket_control_pending_slot_payload()
    }

    // This raw draft remains below the complete-context representation boundary.
    fn draft(&self) -> &crate::metadata_command::MetadataCommandEnvelope {
        match &self.context.input {
            Input::Ordinary(command) => command,
            Input::Described(candidate) => candidate.command(),
            Input::AbandonedPart(candidate) => candidate.command(),
        }
    }

    pub(in super::super::super::super) fn require_install_authorization(
        &self,
        pg: &PgStore,
    ) -> Result<(), StoreError> {
        pg.require_mark_bucket_deleting_install_authorization(
            self.draft(),
            Some(self.scope_bucket()),
            crate::clock::current_time_millis(),
        )
    }

    pub(in super::super::super::super) fn validate_slot_bytes(
        &self,
        bytes: &[u8],
        checksum: u64,
        id: MetadataCommandId,
    ) -> bool {
        super::super::ContextualCommand::decode_command(bytes, checksum, &self.context.topology)
            .is_ok_and(|context| context.id() == id)
    }

    pub(in super::super::super::super) fn prepare_pending<'a>(
        &'a self,
        transaction: &'a PendingInstallationTransaction<'_>,
        node_id: u32,
        topology: &'a PgTopology,
    ) -> Result<CompletePendingProjection<'a>, StoreError> {
        let pg = transaction.store();
        transaction.require_store(pg)?;
        Ok(CompletePendingProjection {
            transaction,
            indexes: self.derive_indexes(pg, node_id, topology, false)?,
        })
    }

    /// Must run inside the startup reader's snapshot. Revalidates the source,
    /// exact routes/window/pages and bucket scope, without exporting installable
    /// inventory or relying on a session that terminal apply may have removed.
    pub(in super::super::super::super) fn require_retained_pending_indexes(
        &self,
        pg: &PgStore,
        node_id: u32,
        topology: &PgTopology,
    ) -> Result<(), StoreError> {
        let indexes = self.derive_indexes(pg, node_id, topology, true)?;
        pg.require_complete_pending_indexes(&indexes, node_id)
    }

    fn derive_indexes<'a>(
        &'a self,
        pg: &'a PgStore,
        node_id: u32,
        topology: &'a PgTopology,
        retained: bool,
    ) -> Result<CompletePendingIndexes<'a>, StoreError> {
        let derive = || -> Result<_, ContextError> {
            // Includes the actual configured topology, PG and open-transaction
            // checks. Never reuse an earlier read-only preparation's inventory.
            let inventory = if retained {
                super::super::inventory::derive_retained(self.context, pg, topology)?
            } else {
                super::super::inventory::derive(self.context, pg, topology)?
            };
            let window = match &self.context.input {
                Input::Described(candidate) => {
                    let bound = candidate.bind(topology)?;
                    let protection = bound.pending_protection();
                    let (_, _, subject, window) = protection.index_inputs();
                    Some((subject.clone(), window.clone()))
                }
                Input::AbandonedPart(candidate) => {
                    candidate
                        .bind(topology)?
                        .require_abandoned_source_in_open_transaction(pg, node_id)?;
                    None
                }
                Input::Ordinary(_) => None,
            };
            Ok(CompletePendingIndexes {
                pg,
                command: self,
                topology,
                window,
                routes: inventory.retention.installation_routes().to_vec(),
                pages: PgStore::encode_pending_payload_reference_pages(&inventory.payload)?,
            })
        };
        derive().map_err(|error| match error {
            ContextError::Retention(error) => error,
            other => StoreError::ShardScavengerScanIncomplete {
                context: "derive complete pending command indexes",
                errors: other.to_string(),
            },
        })
    }
}

impl CompletePendingProjection<'_> {
    pub(in super::super::super::super) fn require_store(
        &self,
        pg: &PgStore,
    ) -> Result<(), StoreError> {
        self.transaction.require_store(pg)
    }
    pub(in super::super::super::super) fn command(&self) -> &EncodedContextualCommand<'_> {
        self.indexes.command()
    }

    pub(in super::super::super::super) fn routes(
        &self,
    ) -> &[PendingMetadataCommandRouteDependency] {
        self.indexes.routes()
    }

    pub(in super::super::super::super) fn pages(&self) -> &PendingShardScavengerReferencePages {
        self.indexes.pages()
    }

    // The index codec can inspect this bound view, but cannot mint it or detach
    // a usable install capability from the complete command it borrows.
    pub(in super::super::super::super) fn index_inputs(
        &self,
    ) -> (&PgTopology, Option<(&StagingSubject, &StagingWindow)>) {
        self.indexes.index_inputs()
    }

    pub(in super::super::super::super) fn require_existing(
        &self,
        pg: &PgStore,
        node_id: u32,
    ) -> Result<(), StoreError> {
        self.require_store(pg)?;
        pg.require_complete_pending_indexes(&self.indexes, node_id)
    }
}

impl CompletePendingIndexes<'_> {
    pub(in super::super::super::super) fn require_store(
        &self,
        pg: &PgStore,
    ) -> Result<(), StoreError> {
        if !std::ptr::eq(pg, self.pg) || pg.conn.is_autocommit() {
            return Err(StoreError::RouteCapabilitySubjectMismatch {
                operation: "complete-pending-index-snapshot",
            });
        }
        Ok(())
    }
    pub(in super::super::super::super) fn command(&self) -> &EncodedContextualCommand<'_> {
        self.command
    }
    pub(in super::super::super::super) fn routes(
        &self,
    ) -> &[PendingMetadataCommandRouteDependency] {
        &self.routes
    }
    pub(in super::super::super::super) fn pages(&self) -> &PendingShardScavengerReferencePages {
        &self.pages
    }
    pub(in super::super::super::super) fn index_inputs(
        &self,
    ) -> (&PgTopology, Option<(&StagingSubject, &StagingWindow)>) {
        (
            self.topology,
            self.window
                .as_ref()
                .map(|(subject, window)| (subject, window)),
        )
    }
}
