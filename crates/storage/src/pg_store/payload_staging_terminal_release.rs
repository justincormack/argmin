// Copyright The Argmin Authors.
// SPDX-License-Identifier: Apache-2.0

//! A command-owned proof never escapes as an independently supplied argument.
//! The only producer executes synchronously under a terminal-holder permit.

use super::{ContextualCommand, Input};
use crate::metadata_command::{
    BucketWriteReservationProof, MetadataCommandPayload as P,
    MetadataCommandReservationDisposition as R,
    COMPLETE_MULTIPART_UPLOAD_BUCKET_WRITE_OPERATION_KIND,
    PUT_OBJECT_STREAM_CREATE_BUCKET_WRITE_OPERATION_KIND,
};
use crate::node_client::RetainedBucketWriteReservationRoute;
use crate::node_runtime::pg_store::TerminalReservationAuthority;
use crate::{BucketName, BucketSnapshotLoadError, ClusterEpoch, StoreError};

pub(crate) struct CompleteTerminalReservationRelease<'a> {
    authority: TerminalReservationAuthority<'a>,
    proof: &'a BucketWriteReservationProof,
}

impl ContextualCommand {
    pub(in crate::node_runtime::pg_store) fn release_terminal_reservation(
        &self,
        authority: TerminalReservationAuthority<'_>,
    ) -> Result<(), BucketSnapshotLoadError> {
        authority.require_context(self)?;
        let command = if authority.is_retained_finalizer() {
            let Input::AbandonedPart(candidate) = &self.input else {
                return Err(StoreError::RouteCapabilitySubjectMismatch {
                    operation: "terminal release requires retained finalizer context",
                }
                .into());
            };
            let source = candidate.source.draft();
            super::abandonment::validate_part_closure_subject(
                &self.topology,
                source,
                candidate.command(),
            )
            .map_err(|_| StoreError::RouteCapabilitySubjectMismatch {
                operation: "terminal finalizer reservation subject",
            })?;
            source
        } else {
            match &self.input {
                Input::Ordinary(command) => command,
                Input::Described(candidate) => candidate.command(),
                Input::AbandonedPart(candidate) => candidate.command(),
            }
        };
        command.payload().validate_terminal_generation_owner()?;
        let proof = match command.payload().bucket_write_reservation_disposition() {
            R::ReleaseOnApply(proof) | R::ReleaseStreamCreationOnApply(proof) => proof,
            R::NoCommandProof | R::RetainForStream(_) => {
                return Err(StoreError::RouteCapabilitySubjectMismatch {
                    operation: "command has no terminal proof release",
                }
                .into())
            }
        };
        let subject_matches = command.bucket_write_reservation_subject_matches(proof)
            && match command.payload() {
                P::AbortStreamUpload(abort) => {
                    abort.has_consistent_segment_subjects()
                        && proof.matches_exact_mutation_subject(
                            proof.cluster_epoch,
                            &abort.bucket,
                            PUT_OBJECT_STREAM_CREATE_BUCKET_WRITE_OPERATION_KIND,
                            Some(abort.key.as_str()),
                        )
                }
                P::CommitMultipartObject(commit) => proof.matches_exact_mutation_subject(
                    command.id().cluster_epoch(),
                    &commit.object.bucket,
                    COMPLETE_MULTIPART_UPLOAD_BUCKET_WRITE_OPERATION_KIND,
                    Some(commit.object.key.as_str()),
                ),
                _ => true,
            };
        if !subject_matches {
            return Err(StoreError::RouteCapabilitySubjectMismatch {
                operation: "terminal command reservation subject",
            }
            .into());
        }
        let release = CompleteTerminalReservationRelease { authority, proof };
        release
            .authority
            .route()
            .release_terminal_reservation(&release)
    }
}

impl CompleteTerminalReservationRelease<'_> {
    pub(crate) fn require_route(
        &self,
        route: &crate::cluster::CompleteFanoutRoute<'_>,
    ) -> Result<(), StoreError> {
        self.authority.require_valid()?;
        if !std::ptr::eq(self.authority.route(), route) {
            return Err(StoreError::RouteCapabilitySubjectMismatch {
                operation: "terminal reservation admitted route identity",
            });
        }
        Ok(())
    }
    pub(crate) fn bucket(&self) -> &BucketName {
        &self.proof.bucket
    }
    pub(crate) fn epoch(&self) -> ClusterEpoch {
        self.proof.cluster_epoch
    }
    pub(crate) fn deadline(&self) -> std::time::Instant {
        self.authority.cutoff().0
    }

    pub(crate) fn send(
        &self,
        route: &dyn RetainedBucketWriteReservationRoute,
    ) -> Result<(), BucketSnapshotLoadError> {
        self.authority.require_valid()?;
        let (local, portable) = self.authority.cutoff();
        // A conflict is not confirmation, including for a stream-creation proof.
        // Exact absence is already idempotent in the storage-owned release API.
        route.release_metadata_command_bucket_write_reservation_with_fixed_deadline(
            self.proof, local, portable,
        )
    }
}
