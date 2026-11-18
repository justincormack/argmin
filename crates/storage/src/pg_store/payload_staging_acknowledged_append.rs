// Copyright The Argmin Authors.
// SPDX-License-Identifier: Apache-2.0

//! Primary snapshot capture for an acknowledged segment. The opaque result has
//! no raw install/encode/extraction API; installation requires the consuming
//! owner's exact-command proof and original admitted section authority.
use super::*;
use crate::metadata_command::{
    MetadataCommandEnvelope, MetadataCommandLogIndex, MetadataCommandPayload,
};
use crate::node_runtime::engine::EmbeddedPublicationAuthority;
use crate::node_runtime::payload_placement::PayloadPlacementInput;
use crate::node_runtime::pg_store::{CompletePublicationRequest, PgStore, StreamAppendPreparation};
use StagingWindowConfirmationError as Error;

pub(in crate::node_runtime) struct CapturedStreamAppend {
    submission: submission::ContextualSubmission,
    #[cfg(test)]
    command: MetadataCommandEnvelope,
}

impl CapturedStreamAppend {
    pub(in crate::node_runtime::pg_store) fn install_remote(
        &mut self,
        proof: &crate::node_runtime::pg_store::StreamAppendInstallationProof<'_, '_>,
        held: &mut crate::node_client::CompletePrimarySessionBorrow<'_>,
    ) -> Result<(), Error> {
        self.submission.install_remote_append(proof, held)
    }
    pub(in crate::node_runtime) fn decode_remote(
        permit: &StreamAppendPreparation<'_, '_>,
        bytes: &[u8],
    ) -> Result<Self, Error> {
        // Reconstruct the request from the opaque custody, not a caller-selected
        // correlation token. No context/submission escapes before comparison.
        let request = permit.remote_request()?;
        let reply = request
            .decode_response(bytes)
            .map_err(|e| Error::Window(e.to_string()))?;
        let original = permit.publication_request()?;
        let context =
            ContextualCommand::decode_command(reply.command, reply.checksum, original.topology())
                .map_err(|e| Error::Window(e.to_string()))?;
        if context.id().pg_id() != original.id().pg_id() {
            return Err(Error::Window("remote append command PG".into()));
        }
        permit.require_remote_context(&context)?;
        #[cfg(test)]
        let command = MetadataCommandEnvelope::new(
            context.id(),
            MetadataCommandPayload::AppendStreamSegment(Box::new(permit.test_command()?)),
        );
        Ok(Self {
            submission: context.into_submission(),
            #[cfg(test)]
            command,
        })
    }

    pub(in crate::node_runtime::pg_store) fn into_publication_sender(
        self,
        primary: crate::NodeId,
        deadline: std::time::Instant,
    ) -> crate::node_runtime::pg_store::CompletePublicationSender {
        self.submission.into_publication_sender(primary, deadline)
    }
    pub(in crate::node_runtime::pg_store) fn installation(
        &self,
    ) -> crate::node_runtime::pg_store::StreamAppendInstallationObservation {
        use crate::node_runtime::pg_store::StreamAppendInstallationObservation as Output;
        use submission::InstallationObservation as Input;
        match self.submission.observation().installation {
            Input::NoAcceptanceObserved => Output::NoAcceptanceObserved,
            Input::Unconfirmed => Output::Unconfirmed,
            Input::Installed => Output::Installed,
        }
    }
    pub(in crate::node_runtime::pg_store) fn digest(&self) -> Result<[u8; 32], Error> {
        self.submission
            .complete_command_sha256()
            .map_err(|e| Error::Window(e.to_string()))
    }
    pub(in crate::node_runtime::pg_store) fn install_in_store(
        &mut self,
        proof: &crate::node_runtime::pg_store::StreamAppendInstallationProof<'_, '_>,
        pg: &PgStore,
        authority: &EmbeddedPublicationAuthority<'_>,
        request: &CompletePublicationRequest<'_>,
    ) -> Result<(), Error> {
        let deadline = authority.stream_append_installation_deadline(proof)?;
        authority.require_effect(pg, request)?;
        let fence = proof.require_captured(self, request)?;
        // Confirmation is historical evidence, not a claim the slot still
        // exists. Later publication revalidates it. Never erase confirmed success
        // or re-enter SQL merely because another installation call was made.
        if self.submission.observation().installation
            == submission::InstallationObservation::Installed
        {
            return Ok(());
        }
        self.submission
            .install(
                pg,
                request.node_id().as_u32(),
                request.topology(),
                fence,
                Some(deadline),
            )
            .map_err(|error| match error {
                submission::SubmissionError::Rejected(error)
                | submission::SubmissionError::Unconfirmed(error) => Error::Store(error),
                other => Error::Window(other.to_string()),
            })
    }
    pub(in crate::node_runtime::pg_store) fn id(&self) -> MetadataCommandId {
        self.submission.observation().command_id
    }
    #[cfg(test)]
    pub(in crate::node_runtime::pg_store) fn test_command(&self) -> MetadataCommandEnvelope {
        self.submission.assert_command_for_test(&self.command);
        self.command.clone()
    }
}

pub(in crate::node_runtime::pg_store) fn capture_acknowledged_append(
    permit: &StreamAppendPreparation<'_, '_>,
    pg: &PgStore,
    authority: &EmbeddedPublicationAuthority<'_>,
    request: &CompletePublicationRequest<'_>,
    placement: PayloadPlacementInput<'_>,
) -> Result<CapturedStreamAppend, Error> {
    authority.require_stream_append_preparation(permit)?;
    authority.require_effect(pg, request)?;
    let snapshot = pg
        .command_build_snapshot()
        .map_err(|e| Error::Window(e.to_string()))?;
    let (command, effect_fence) = permit.command_in_store(pg, authority, request)?;
    pg.validate_append_stream_segment_command(&command)
        .map_err(|e| Error::Window(e.to_string()))?
        .require_primary_preparation()?;
    let index = pg
        .max_metadata_command_log_index(request.id().cluster_epoch())?
        .checked_add(1)
        .and_then(MetadataCommandLogIndex::new)
        .ok_or_else(|| Error::Window("append command index exhausted".into()))?;
    let command = MetadataCommandEnvelope::new(
        MetadataCommandId::new(request.id().cluster_epoch(), request.id().pg_id(), index),
        MetadataCommandPayload::AppendStreamSegment(Box::new(command)),
    );
    let capture = capture::StagingCapture {
        topology: request.topology(),
        placement,
        effect_fence,
    };
    #[cfg(test)]
    let expected_command = command.clone();
    let candidate = capture
        .append_candidate(pg, request.node_id(), command)
        .map_err(|e| Error::Window(e.to_string()))?;
    let context = ContextualCommand::from_described(candidate);
    context
        .observe_pending_inventory_in_open_transaction(pg, request.topology())
        .map_err(|e| Error::Window(e.to_string()))?;
    snapshot
        .finish()
        .map_err(|e| Error::Window(e.to_string()))?;
    authority.require_stream_append_preparation(permit)?;
    authority.require_effect(pg, request)?;
    pg.require_live_allocation_lifetime()?;
    Ok(CapturedStreamAppend {
        submission: context.into_submission(),
        #[cfg(test)]
        command: expected_command,
    })
}
