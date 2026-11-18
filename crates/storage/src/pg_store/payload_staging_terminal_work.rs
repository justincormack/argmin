// Copyright The Argmin Authors.
// SPDX-License-Identifier: Apache-2.0

//! Scalar diagnostics derived from the retained complete context. These are not
//! release/deletion authority, an installable inventory, or proof of cleanup.
//! Exact resources and source bytes remain inaccessible inside their owner.

use super::{ContextualCommand, Input};
use crate::metadata_command::{
    MetadataCommandPayload as P, MetadataCommandReservationDisposition as R,
};

/// Eligibility derived only from the retained complete command, never supplied
/// by a caller. Creation and Direct PUT additionally require confirmed release
/// at the sender and exact durable custody at the receiver, including an
/// overwrite's independent displaced-payload reclaim handoff.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(in crate::node_runtime::pg_store) enum TerminalSlotWork {
    Quiescent,
    UploadPartCreation,
    DirectPutPublication,
    Gated,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(in crate::node_runtime) enum TerminalReservationWork {
    NoCommandProof,
    RetainForStream,
    ReleaseCommandProof,
    ReleaseStreamCreationProof,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(in crate::node_runtime) enum TerminalPayloadWork {
    None,
    StreamAbort {
        segments: usize,
    },
    PartReplacement {
        segments: usize,
    },
    MultipartAbort {
        parts: usize,
        part_segments: usize,
        sessions: usize,
        session_segments: usize,
    },
    // Completion's physical exclusion and autonomous retry remain separate
    // activation gates. This is deliberately not folded into None or a claim
    // that retained owner rows alone permit pending-slot removal.
    MultipartCompletion {
        omitted_parts: usize,
        omitted_segments: usize,
        sessions: usize,
        session_segments: usize,
    },
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(in crate::node_runtime) struct TerminalWorkObservation {
    pub(in crate::node_runtime) reservation: TerminalReservationWork,
    pub(in crate::node_runtime) payload: TerminalPayloadWork,
    /// Applied metadata hands displaced payload to the durable reclaim path;
    /// this is not a request to delete that payload from the command holder.
    pub(in crate::node_runtime) durable_reclaim_handoff: bool,
    /// Source-bound abandonment retains an additional exact finalizer proof in
    /// durable owner state, even when the abort carries no creation proof.
    pub(in crate::node_runtime) retained_finalization_proof: bool,
}

impl TerminalWorkObservation {
    pub(in crate::node_runtime::pg_store) fn has_reservation_release(self) -> bool {
        matches!(
            self.reservation,
            TerminalReservationWork::ReleaseCommandProof
                | TerminalReservationWork::ReleaseStreamCreationProof
        ) || self.retained_finalization_proof
    }

    // Eligibility only, never a release/removal capability. Both the sender and
    // the receiver derive this from their retained complete context.
    pub(in crate::node_runtime::pg_store) fn is_quiescent(self) -> bool {
        self.payload == TerminalPayloadWork::None
            && matches!(
                self.reservation,
                TerminalReservationWork::NoCommandProof | TerminalReservationWork::RetainForStream
            )
            && !self.durable_reclaim_handoff
            && !self.retained_finalization_proof
    }
}

impl ContextualCommand {
    pub(in crate::node_runtime::pg_store) fn terminal_slot_work(&self) -> TerminalSlotWork {
        if self.terminal_work().is_quiescent() {
            TerminalSlotWork::Quiescent
        } else if matches!(&self.input, Input::Described(candidate)
            if matches!(candidate.command().payload(), P::CreateStreamUpload(create)
                if matches!(create.session.target, crate::StreamUploadTarget::UploadPart { .. })))
        {
            TerminalSlotWork::UploadPartCreation
        } else if matches!(&self.input, Input::Ordinary(command)
            if matches!(command.payload(), P::CommitDirectPutObject(commit)
                if commit.bucket_write_reservation.operation_kind
                        == crate::metadata_command::PUT_OBJECT_DIRECT_COMMIT_BUCKET_WRITE_OPERATION_KIND))
        {
            TerminalSlotWork::DirectPutPublication
        } else {
            TerminalSlotWork::Gated
        }
    }

    pub(in crate::node_runtime::pg_store) fn require_slot_retirement_custody(
        &self,
        pg: &crate::node_runtime::pg_store::PgStore,
    ) -> Result<(), super::ContextError> {
        match self.terminal_slot_work() {
            TerminalSlotWork::Quiescent => {
                // Direct introduction has no terminal command proof to release,
                // but its slot still protects the exact durable cleanup owner.
                // Quiescent work must not mean that missing custody is valid.
                if let Input::Described(candidate) = &self.input {
                    if matches!(candidate.command().payload(), P::ReserveObjectGeneration(_)) {
                        candidate
                            .bind(&self.topology)?
                            .require_creation_retirement(pg)?;
                    }
                }
                Ok(())
            }
            TerminalSlotWork::UploadPartCreation => {
                let Input::Described(candidate) = &self.input else {
                    unreachable!("classified described creation")
                };
                candidate
                    .bind(&self.topology)?
                    .require_creation_retirement(pg)?;
                Ok(())
            }
            TerminalSlotWork::DirectPutPublication => {
                let Input::Ordinary(command) = &self.input else {
                    unreachable!("classified ordinary Direct PUT")
                };
                super::super::object::require_direct_retirement(pg, &self.topology, command)?;
                Ok(())
            }
            TerminalSlotWork::Gated => Err(super::ContextError::CommandKind),
        }
    }

    pub(in crate::node_runtime::pg_store) fn terminal_work(&self) -> TerminalWorkObservation {
        let (command, retained_finalization_proof) = match &self.input {
            Input::Ordinary(command) => (command, false),
            Input::Described(candidate) => (candidate.command(), false),
            Input::AbandonedPart(candidate) => (candidate.command(), true),
        };
        let reservation = match command.payload().bucket_write_reservation_disposition() {
            R::NoCommandProof => TerminalReservationWork::NoCommandProof,
            R::RetainForStream(_) => TerminalReservationWork::RetainForStream,
            R::ReleaseOnApply(_) => TerminalReservationWork::ReleaseCommandProof,
            R::ReleaseStreamCreationOnApply(_) => {
                TerminalReservationWork::ReleaseStreamCreationProof
            }
        };
        // No wildcard: every new command needs an explicit terminal-work decision.
        let (payload, durable_reclaim_handoff) = match command.payload() {
            P::AbortStreamUpload(abort) => (
                TerminalPayloadWork::StreamAbort {
                    segments: abort.staged_segments.len(),
                },
                false,
            ),
            P::CommitStreamPart(commit) => (
                TerminalPayloadWork::PartReplacement {
                    segments: commit.displaced_segments.len(),
                },
                false,
            ),
            P::AbortMultipartUpload(abort) => (
                TerminalPayloadWork::MultipartAbort {
                    parts: abort.cleanup.parts.len(),
                    part_segments: abort.cleanup.streaming_segments.len(),
                    sessions: abort.cleanup.stream_uploads.len(),
                    session_segments: abort.cleanup.stream_upload_segments.len(),
                },
                false,
            ),
            P::CommitMultipartObject(commit) => (
                TerminalPayloadWork::MultipartCompletion {
                    omitted_parts: commit.omitted_parts.len(),
                    omitted_segments: commit.omitted_streaming_segments.len(),
                    sessions: commit.stream_uploads.len(),
                    session_segments: commit.stream_upload_segments.len(),
                },
                commit.stale_payload.is_some(),
            ),
            P::CommitDirectPutObject(commit) => {
                (TerminalPayloadWork::None, commit.stale_payload.is_some())
            }
            P::InsertDeleteMarker(marker) => {
                (TerminalPayloadWork::None, marker.stale_payload.is_some())
            }
            P::DeleteObjectVersion(delete) => (
                TerminalPayloadWork::None,
                match &delete.target {
                    crate::metadata_command::DeleteObjectVersionTarget::DeleteMarker { .. } => {
                        false
                    }
                    crate::metadata_command::DeleteObjectVersionTarget::Live { .. } => true,
                },
            ),
            P::CreateBucket(_)
            | P::PutBucketVersioning(_)
            | P::PutBucketAcl(_)
            | P::PutBucketProperty(_)
            | P::PutBucketSubresource(_)
            | P::MarkBucketDeleting(_)
            | P::DeleteFinalizedBucket(_)
            | P::ReserveObjectGeneration(_)
            | P::ReleaseObjectGeneration(_)
            | P::ReserveObjectVersion(_)
            | P::PutObjectMetadata(_)
            | P::CreateStreamUpload(_)
            | P::AppendStreamSegment(_)
            | P::CreateMultipartUpload(_)
            | P::DeleteObjectPayloadReclaim(_)
            | P::AdvanceMultipartCompletionBarrier(_) => (TerminalPayloadWork::None, false),
        };
        TerminalWorkObservation {
            reservation,
            payload,
            durable_reclaim_handoff,
            retained_finalization_proof,
        }
    }
}
