// Copyright The Argmin Authors.
// SPDX-License-Identifier: Apache-2.0

//! Exact terminal identity, never independently supplied ID/checksum/digest.
//! The live adapter retains its current envelope; coordinated integration
//! retains the complete carrier. Neither constructor confers recovery or
//! abandonment authority. Keep this representation private to the log owner.

use super::*;
use crate::node_runtime::pg_store::payload_staging::{ContextualCommand, EncodedContextualCommand};
use crate::PgTopology;
use std::borrow::Cow;

enum Origin<'a> {
    Current(&'a MetadataCommandEnvelope),
    Complete(&'a EncodedContextualCommand<'a>),
}

pub(super) struct TerminalCommandIdentity<'a> {
    origin: Origin<'a>,
}

/// Selected explicitly by the retained command or complete log reader, never
/// by trying both decoders. Tombstones share their independent grammar;
/// applied entries must use the selected one.
#[derive(Clone, Copy)]
pub(super) enum TerminalLogGrammar<'a> {
    Current,
    Complete(&'a PgTopology),
}

impl TerminalLogGrammar<'_> {
    /// Maintenance must not discard any pending custody. The complete path
    /// checks the complete format and bucket scope even for an older epoch,
    /// but does not classify convergence or validate/remint installation indexes.
    pub(super) fn has_pending(
        self,
        pg: &PgStore,
        node_id: u32,
        cluster_epoch: ClusterEpoch,
    ) -> Result<bool, StoreError> {
        let Self::Complete(topology) = self else {
            return Ok(pg
                .pending_metadata_command_slot(node_id, cluster_epoch)?
                .is_some());
        };
        let mut expected_scope = None;
        let slot =
            pg.pending_metadata_command_slot_with_validator(node_id, |bytes, checksum, id| {
                let conflict = || StoreError::MetadataCommandLogConflict {
                    node_id,
                    pg_id: pg.pg_id,
                    cluster_epoch: id.cluster_epoch(),
                    log_index: id.log_index().get(),
                };
                let context = ContextualCommand::decode_command(bytes, checksum, topology)
                    .map_err(|_| conflict())?;
                if context.id() != id {
                    return Err(conflict());
                }
                expected_scope = Some(context.scope_bucket().clone());
                Ok(())
            })?;
        let Some(slot) = slot else {
            return Ok(false);
        };
        if slot.scope_bucket != expected_scope {
            return Err(StoreError::RouteCapabilitySubjectMismatch {
                operation: "complete log maintenance pending bucket scope",
            });
        }
        Ok(true)
    }

    pub(super) fn verify(
        self,
        pg: &PgStore,
        node_id: u32,
        id: MetadataCommandId,
        entry: &MetadataCommandLogEntry,
    ) -> Result<(), StoreError> {
        let Self::Complete(topology) = self else {
            return pg.verify_metadata_command_log_entry(
                node_id,
                id.cluster_epoch(),
                id.pg_id(),
                id.log_index(),
                entry,
            );
        };
        if entry.abandoned {
            return pg.verify_metadata_command_log_entry(
                node_id,
                id.cluster_epoch(),
                id.pg_id(),
                id.log_index(),
                entry,
            );
        }
        Self::decode_complete_applied(pg, node_id, id, entry, topology).map(|_| ())
    }

    /// Shared with range inspection. Only the complete retained variant leaves
    /// the decoder; no caller can extract an unauthenticated payload draft.
    pub(super) fn decode_complete_applied(
        pg: &PgStore,
        node_id: u32,
        id: MetadataCommandId,
        entry: &MetadataCommandLogEntry,
        topology: &PgTopology,
    ) -> Result<ContextualCommand, StoreError> {
        let computed_checksum = checksum::crc64::checksum(&entry.command_bytes);
        if computed_checksum != entry.command_checksum {
            return Err(StoreError::MetadataCommandLogChecksumMismatch {
                node_id,
                pg_id: pg.pg_id,
                cluster_epoch: id.cluster_epoch(),
                log_index: id.log_index().get(),
                stored_checksum: entry.command_checksum,
                computed_checksum,
            });
        }
        let conflict = || StoreError::MetadataCommandLogConflict {
            node_id,
            pg_id: pg.pg_id,
            cluster_epoch: id.cluster_epoch(),
            log_index: id.log_index().get(),
        };
        if entry.abandoned || id.pg_id().get() != pg.pg_id {
            return Err(conflict());
        }
        let context = ContextualCommand::decode_command(
            &entry.command_bytes,
            entry.command_checksum,
            topology,
        )
        .map_err(|_| conflict())?;
        if context.id() != id {
            return Err(conflict());
        }
        Ok(context)
    }
}

impl<'a> TerminalCommandIdentity<'a> {
    pub(super) fn grammar(&self) -> TerminalLogGrammar<'_> {
        match self.origin {
            Origin::Current(_) => TerminalLogGrammar::Current,
            Origin::Complete(command) => TerminalLogGrammar::Complete(command.topology()),
        }
    }
    pub(super) fn current(command: &'a MetadataCommandEnvelope) -> Self {
        Self {
            origin: Origin::Current(command),
        }
    }

    pub(super) fn complete(command: &'a EncodedContextualCommand<'a>) -> Self {
        Self {
            origin: Origin::Complete(command),
        }
    }

    pub(super) fn id(&self) -> MetadataCommandId {
        match self.origin {
            Origin::Current(command) => command.id(),
            Origin::Complete(command) => command.id(),
        }
    }

    fn checksum(&self) -> u64 {
        match self.origin {
            Origin::Current(command) => command.checksum_crc64(),
            Origin::Complete(command) => command.checksum_crc64(),
        }
    }

    pub(super) fn validate_terminal_generation_owner(&self) -> Result<(), StoreError> {
        match self.origin {
            Origin::Current(command) => command.payload().validate_terminal_generation_owner(),
            Origin::Complete(command) => command.validate_terminal_generation_owner(),
        }
    }

    pub(super) fn record(&self, abandoned: bool) -> (u64, Cow<'_, [u8]>) {
        // Delay serialization until an actual record/receipt comparison. An
        // absent source lookup must not newly encode a multi-MiB command.
        let original = match self.origin {
            Origin::Current(command) => Cow::Owned(command.command_bytes()),
            Origin::Complete(command) => Cow::Borrowed(command.bytes()),
        };
        if abandoned {
            let bytes = abandoned_command_log_bytes(
                self.id(),
                self.checksum(),
                checksum::sha256::digest(&original),
            );
            (checksum::crc64::checksum(&bytes), Cow::Owned(bytes))
        } else {
            (self.checksum(), original)
        }
    }

    pub(super) fn conflict(&self, node_id: u32, pg_id: u32) -> StoreError {
        StoreError::MetadataCommandLogConflict {
            node_id,
            pg_id,
            cluster_epoch: self.id().cluster_epoch(),
            log_index: self.id().log_index().get(),
        }
    }
}
