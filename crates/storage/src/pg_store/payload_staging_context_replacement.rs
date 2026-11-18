// Copyright The Argmin Authors.
// SPDX-License-Identifier: Apache-2.0

//! The complete replacement boundary borrows the owning history. Encoding a
//! source and successor independently cannot mint this capability. Private
//! construction and consuming execution prevent detaching a raw mutation
//! result from the retained installation observations.

use super::{ContextualReissue, InstallationObservation};
use crate::node_runtime::pg_store::payload_staging::{EncodedContextualCommand, SubmissionError};
use crate::node_runtime::pg_store::{PendingMetadataCommandSlotReplaceError, PgStore};
use crate::PgTopology;

/// No Clone, extraction, raw-pair constructor or standalone history. Its
/// exclusive borrow prevents replacing/dropping the owning pair while the
/// storage operation runs. Only the owning submission module can mint it.
pub(in super::super::super::super) struct CompletePendingReplacement<'a> {
    owner: &'a mut ContextualReissue,
    topology: &'a PgTopology,
}

impl<'a> CompletePendingReplacement<'a> {
    pub(super) fn new(owner: &'a mut ContextualReissue, topology: &'a PgTopology) -> Self {
        Self { owner, topology }
    }

    /// The store supplies only its private transaction implementation. This
    /// scope derives both encodings from the borrowed owner and records every
    /// result before releasing that borrow. A panic leaves Unconfirmed, while
    /// false/definitive outcomes restore prior history rather than reset it.
    pub(in super::super::super::super) fn execute(
        self,
        pg: &PgStore,
        operation: impl FnOnce(
            &EncodedContextualCommand<'_>,
            &EncodedContextualCommand<'_>,
            &PgTopology,
        ) -> Result<bool, PendingMetadataCommandSlotReplaceError>,
    ) -> Result<bool, SubmissionError> {
        use PendingMetadataCommandSlotReplaceError as Failure;
        if let Err(error) = pg.require_usable_connection() {
            self.owner.successor.installation = InstallationObservation::Unconfirmed;
            return Err(SubmissionError::Unconfirmed(error));
        }
        // A released savepoint is not a durable replacement observation. This
        // check belongs at the actual capability boundary, not only its caller.
        if !pg.conn.is_autocommit() {
            return Err(SubmissionError::OpenTransaction);
        }
        let source = self.owner.source.context.encode_command()?;
        let successor = self.owner.successor.context.encode_command()?;
        let previous = self.owner.successor.installation;
        self.owner.successor.installation = InstallationObservation::Unconfirmed;
        match operation(&source, &successor, self.topology) {
            Ok(true) => {
                self.owner.successor.installation = InstallationObservation::Installed;
                Ok(true)
            }
            Ok(false) => {
                self.owner.successor.installation = previous;
                Ok(false)
            }
            Err(Failure::Definitive(error)) => {
                self.owner.successor.installation = previous;
                Err(SubmissionError::Rejected(error))
            }
            Err(Failure::MayHaveApplied(error)) => Err(SubmissionError::Unconfirmed(error)),
        }
    }
}
