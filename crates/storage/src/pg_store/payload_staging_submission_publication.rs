// Copyright The Argmin Authors.
// SPDX-License-Identifier: Apache-2.0

//! One-way transfer of a complete submission AND its installation history.
//! This private owner never lends an input that can recreate a fresh submission.

use super::{ContextSubmissionObservation, ContextualSubmission};
use crate::node_runtime::pg_store::payload_staging::command_context::CarrierError;
use crate::node_runtime::pg_store::payload_staging::EncodedContextualCommand;
use crate::node_runtime::pg_store::CompletePublicationSender;
use crate::NodeId;
use std::time::Instant;

pub(in crate::node_runtime::pg_store) struct SubmittedPublicationEvidence {
    submission: ContextualSubmission,
}

impl ContextualSubmission {
    pub(in crate::node_runtime::pg_store) fn into_publication_sender_with_cutoff(
        self,
        node_id: NodeId,
        local: Instant,
        portable: crate::storage_rpc::StorageRpcOperationDeadline,
    ) -> CompletePublicationSender {
        CompletePublicationSender::from_submission_with_cutoff(
            SubmittedPublicationEvidence { submission: self },
            node_id,
            local,
            portable,
        )
    }

    /// No reinspection, decode/re-encode round trip, mutation or caller-selected
    /// observation. The returned sender cannot be converted back into a fresh
    /// submission, even if its publication attempt is rejected or never sent.
    pub(in crate::node_runtime::pg_store) fn into_publication_sender(
        self,
        node_id: NodeId,
        deadline: Instant,
    ) -> CompletePublicationSender {
        CompletePublicationSender::from_submission(
            SubmittedPublicationEvidence { submission: self },
            node_id,
            deadline,
        )
    }
}

// This opaque value has no public producer or extractor. Only the consuming
// handoff above constructs it, and the publication sender keeps it private.
impl SubmittedPublicationEvidence {
    pub(in crate::node_runtime::pg_store) fn encode_stream_allocation(
        &self,
        node: crate::NodeId,
        read: &super::super::super::CapturedWindowRead,
        request: &crate::PrepareStreamUploadSegmentAppendReq,
        expected: &crate::types::ExpectedStreamAppendState,
    ) -> Result<Vec<u8>, crate::node_runtime::pg_store::StagingWindowConfirmationError> {
        self.submission
            .context
            .encode_stream_allocation(node, read, request, expected)
    }
    pub(in crate::node_runtime::pg_store) fn require_stream_allocation_window(
        &self,
        pg: &crate::node_runtime::pg_store::PgStore,
        request: &crate::PrepareStreamUploadSegmentAppendReq,
    ) -> Result<(), crate::node_runtime::pg_store::StagingWindowConfirmationError> {
        self.submission
            .context
            .require_stream_allocation_window(pg, request)
    }
    pub(in crate::node_runtime::pg_store) fn capture_window_read_embedded(
        &self,
        section: &crate::cluster::EmbeddedPublicationSection<'_>,
        deadline: Instant,
        #[cfg(test)] hook: impl FnMut(&'static str),
    ) -> Result<
        super::super::super::CapturedWindowRead,
        crate::node_runtime::pg_store::StagingWindowConfirmationError,
    > {
        self.submission.context.capture_window_read_embedded(
            section,
            deadline,
            #[cfg(test)]
            hook,
        )
    }

    pub(in crate::node_runtime::pg_store) fn capture_window_read_remote(
        &self,
        route: &crate::cluster::CompleteFanoutRoute<'_>,
        client: &crate::node_client::UnixStorageNodeClient,
        session: &crate::node_client::UnixStorageNodeMetadataCommandSession,
        deadline: Instant,
        #[cfg(test)] hook: impl FnMut(&'static str),
    ) -> Result<
        super::super::super::CapturedWindowRead,
        crate::node_runtime::pg_store::StagingWindowConfirmationError,
    > {
        self.submission.context.capture_window_read_remote(
            route,
            client,
            session,
            deadline,
            #[cfg(test)]
            hook,
        )
    }

    pub(in crate::node_runtime::pg_store) fn terminal_slot_work(
        &self,
    ) -> super::super::super::TerminalSlotWork {
        self.submission.context.terminal_slot_work()
    }

    pub(in crate::node_runtime::pg_store) fn release_terminal_reservation(
        &self,
        authority: crate::node_runtime::pg_store::TerminalReservationAuthority<'_>,
    ) -> Result<(), crate::BucketSnapshotLoadError> {
        self.submission
            .context
            .release_terminal_reservation(authority)
    }
    pub(in crate::node_runtime::pg_store) fn terminal_work(
        &self,
    ) -> super::super::super::TerminalWorkObservation {
        self.submission.context.terminal_work()
    }
    pub(in crate::node_runtime::pg_store) fn observation(&self) -> ContextSubmissionObservation {
        self.submission.observation()
    }

    pub(in crate::node_runtime::pg_store) fn encode(
        &self,
    ) -> Result<EncodedContextualCommand<'_>, CarrierError> {
        self.submission.context.encode_command()
    }
}
