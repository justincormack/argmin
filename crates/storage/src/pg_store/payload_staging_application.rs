// Copyright The Argmin Authors.
// SPDX-License-Identifier: Apache-2.0

//! Exhaustive fresh-application routing for the coordinated ownership switch.
//! This does not perform command acceptance, record the applied prefix, or mint
//! recovery authority. The enclosing authenticated carrier/acceptance boundary
//! must select the input, including mandatory abandonment-source context. Current
//! production dispatch is NOT switched: v13 cannot authenticate that context or
//! the captured descriptors. There is no owner-exists/old-path fallback here.

use super::super::abandonment::{self, source::BoundPartCleanup};
use super::super::candidate::{BoundStagingCandidate, CandidateApplied, CandidateBindingError};
use super::super::{close, delete, marker, multipart, object, part, reclaim};
use super::{ContextualCommand, Input};
use crate::metadata_command::{MetadataCommandEnvelope, MetadataCommandPayload};
use crate::node_runtime::pg_store::PgStore;
use crate::{MetadataError, PgTopology};

#[derive(Debug, thiserror::Error)]
pub(in super::super::super) enum ApplicationError {
    #[error("staging command application requires an enclosing transaction")]
    TransactionRequired,
    #[error("staging command application has a crossed PG or invalid checksum")]
    Command,
    #[error("staging command application requires its captured descriptor")]
    DescriptorRequired,
    #[error(transparent)]
    Candidate(#[from] CandidateBindingError),
    #[error(transparent)]
    Close(#[from] close::CloseError),
    #[error(transparent)]
    Object(#[from] object::ObjectError),
    #[error(transparent)]
    Part(#[from] part::PartError),
    #[error(transparent)]
    Multipart(#[from] multipart::MultipartTerminalError),
    #[error(transparent)]
    Delete(#[from] delete::DeleteError),
    #[error(transparent)]
    Marker(#[from] marker::MarkerError),
    #[error(transparent)]
    Reclaim(#[from] reclaim::ReclaimError),
    #[error(transparent)]
    Abandonment(#[from] abandonment::AbandonmentError),
    #[error(transparent)]
    Metadata(#[from] MetadataError),
}

#[derive(Debug, PartialEq, Eq)]
pub(in super::super::super) enum Applied {
    Described(CandidateApplied),
    Existing,
    Abandonment(abandonment::AbandonmentOutcome),
}

/// No wildcard: a new command variant requires an explicit ownership decision.
/// `Unchanged` means no retained-owner mutation, not absence of ordinary state
/// changes, reservation release responsibilities or command acceptance checks.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ExistingHandler {
    Unchanged,
    BucketFinalization,
    Close,
    Object,
    Part,
    Multipart,
    Delete,
    Marker,
    Reclaim,
}

impl ExistingHandler {
    fn for_command(command: &MetadataCommandEnvelope) -> Result<Self, ApplicationError> {
        use MetadataCommandPayload as P;
        Ok(match command.payload() {
            P::ReserveObjectGeneration(_)
            | P::CreateStreamUpload(_)
            | P::AppendStreamSegment(_) => {
                return Err(ApplicationError::DescriptorRequired);
            }
            P::ReleaseObjectGeneration(_) | P::AbortStreamUpload(_) => Self::Close,
            P::CommitDirectPutObject(_) => Self::Object,
            P::CommitStreamPart(_) => Self::Part,
            P::CommitMultipartObject(_) | P::AbortMultipartUpload(_) => Self::Multipart,
            P::DeleteObjectVersion(_) => Self::Delete,
            P::InsertDeleteMarker(_) => Self::Marker,
            P::DeleteObjectPayloadReclaim(_) => Self::Reclaim,
            P::DeleteFinalizedBucket(_) => Self::BucketFinalization,
            P::CreateBucket(_)
            | P::PutBucketVersioning(_)
            | P::PutBucketAcl(_)
            | P::PutBucketProperty(_)
            | P::PutBucketSubresource(_)
            | P::MarkBucketDeleting(_)
            | P::ReserveObjectVersion(_)
            | P::PutObjectMetadata(_)
            | P::CreateMultipartUpload(_)
            | P::AdvanceMultipartCompletionBarrier(_) => Self::Unchanged,
        })
    }
}

enum Handler<'a> {
    Described(BoundStagingCandidate<'a>),
    Existing(ExistingHandler),
    AbandonedPart(BoundPartCleanup<'a>),
}

/// Keeps the selected handler and its exact inputs together. Binding does not
/// cache live ownership, evaluate clocks or establish terminal-source evidence.
pub(in super::super::super) struct CommandApplication<'a> {
    command: &'a MetadataCommandEnvelope,
    topology: &'a PgTopology,
    handler: Handler<'a>,
}

impl<'a> CommandApplication<'a> {
    /// Only the owning context module can construct application. All inputs,
    /// including the ordinary/abandoned distinction, come from its private
    /// retained variant. There is no raw-envelope constructor to downgrade it.
    pub(super) fn from_context(
        context: &'a ContextualCommand,
        topology: &'a PgTopology,
    ) -> Result<Self, ApplicationError> {
        if topology != &context.topology {
            return Err(CandidateBindingError::Topology.into());
        }
        let (command, handler) = match &context.input {
            Input::Ordinary(command) => (
                command,
                Handler::Existing(ExistingHandler::for_command(command)?),
            ),
            Input::Described(candidate) => (
                candidate.command(),
                Handler::Described(candidate.bind(topology)?),
            ),
            Input::AbandonedPart(candidate) => (
                candidate.command(),
                Handler::AbandonedPart(candidate.bind(topology)?),
            ),
        };
        Ok(Self {
            command,
            topology,
            handler,
        })
    }

    pub(in super::super::super) fn apply_in_open_transaction(
        self,
        pg: &PgStore,
        node_id: u32,
    ) -> Result<Applied, ApplicationError> {
        if pg.conn.is_autocommit() {
            return Err(ApplicationError::TransactionRequired);
        }
        if self.command.id().pg_id().get() != pg.pg_id
            || !self.topology.contains_pg(self.command.id().pg_id())
            || !self.command.verify_checksum()
        {
            return Err(ApplicationError::Command);
        }
        match self.handler {
            Handler::Described(bound) => {
                Ok(Applied::Described(bound.apply_in_open_transaction(pg)?))
            }
            Handler::AbandonedPart(bound) => Ok(Applied::Abandonment(
                bound.apply_in_open_transaction(pg, node_id)?,
            )),
            Handler::Existing(handler) => {
                match handler {
                    // Finalization uses the shared deepest bucket deletion
                    // transaction, which preserves every owner-backed key's
                    // generation fence. It is not ownership-independent even
                    // though it leaves the owner rows themselves unchanged.
                    ExistingHandler::BucketFinalization | ExistingHandler::Unchanged => {
                        pg.apply_metadata_command(self.command)?
                    }
                    ExistingHandler::Close => {
                        close::apply_in_open_transaction(pg, self.topology, self.command)?
                    }
                    ExistingHandler::Object => {
                        object::apply_in_open_transaction(pg, self.topology, self.command)?
                    }
                    ExistingHandler::Part => {
                        part::apply_in_open_transaction(pg, self.topology, self.command)?
                    }
                    ExistingHandler::Multipart => {
                        multipart::apply_in_open_transaction(pg, self.topology, self.command)?
                    }
                    ExistingHandler::Delete => {
                        delete::apply_in_open_transaction(pg, self.topology, self.command)?
                    }
                    ExistingHandler::Marker => {
                        marker::apply_in_open_transaction(pg, self.topology, self.command)?
                    }
                    ExistingHandler::Reclaim => {
                        reclaim::apply_in_open_transaction(pg, self.topology, self.command)?
                    }
                }
                Ok(Applied::Existing)
            }
        }
    }
}
