// Copyright The Argmin Authors.
// SPDX-License-Identifier: Apache-2.0

//! The ordinary context is minted only from the admitted consuming preparation,
//! never by decoding a caller's command or accepting an independent envelope.
use super::super::StagingWindowConfirmationError as Error;
use super::*;
use crate::metadata_command::MetadataCommandLogIndex;
use crate::node_runtime::engine::EmbeddedPublicationAuthority;
use crate::node_runtime::pg_store::{
    CompletePublicationRequest, DirectTerminalPreparation, PgStore,
};

pub(in crate::node_runtime) struct CapturedDirectTerminal {
    submission: super::super::submission::ContextualSubmission,
    remote_preparation: Option<crate::storage_rpc::complete_direct_prepare::EncodedRequest>,
    #[cfg(test)]
    command: MetadataCommandEnvelope,
}

impl CapturedDirectTerminal {
    pub(in crate::node_runtime) fn decode_remote(
        permit: &DirectTerminalPreparation<'_, '_>,
        held: &crate::node_client::CompletePrimarySessionBorrow<'_>,
        bytes: &[u8],
    ) -> Result<Self, Error> {
        permit.require_remote_session(held)?;
        let request = permit.remote_request()?;
        let reply = request
            .decode_response(bytes)
            .map_err(|e| Error::Window(e.to_string()))?;
        let original = permit.publication_request()?;
        let context =
            ContextualCommand::decode_command(reply.command, reply.checksum, original.topology())
                .map_err(|e| Error::Window(e.to_string()))?;
        let Input::Ordinary(command) = &context.input else {
            return Err(Error::Window("remote direct terminal context kind".into()));
        };
        if command.id().pg_id() != original.id().pg_id()
            || command.id().cluster_epoch() != original.id().cluster_epoch()
            || command.id().log_index() <= original.id().log_index()
        {
            return Err(Error::Window(
                "remote direct terminal command position".into(),
            ));
        }
        let MetadataCommandPayload::CommitDirectPutObject(commit) = command.payload() else {
            return Err(Error::Window("remote direct terminal command kind".into()));
        };
        permit.require_remote_command(commit)?;
        permit.require_remote_session(held)?;
        #[cfg(test)]
        let command = command.clone();
        Ok(Self {
            submission: context.into_submission(),
            remote_preparation: Some(request),
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
    ) -> crate::node_runtime::pg_store::DirectTerminalInstallationObservation {
        use super::super::submission::InstallationObservation as Input;
        use crate::node_runtime::pg_store::DirectTerminalInstallationObservation as Output;
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
    pub(in crate::node_runtime::pg_store) fn remote_preparation_digest(
        &self,
    ) -> Result<[u8; 32], Error> {
        self.remote_preparation
            .as_ref()
            .map(|r| crate::storage_rpc::complete_stream_allocation::digest(r.bytes()))
            .ok_or_else(|| Error::Window("direct installation requires remote preparation".into()))
    }
    pub(in crate::node_runtime::pg_store) fn install_remote(
        &mut self,
        proof: &crate::node_runtime::pg_store::DirectTerminalInstallationProof<'_, '_>,
        held: &mut crate::node_client::CompletePrimarySessionBorrow<'_>,
    ) -> Result<(), Error> {
        let preparation = self.remote_preparation.as_ref().ok_or_else(|| {
            Error::Window("direct installation requires remote preparation".into())
        })?;
        self.submission
            .install_remote_direct(proof, preparation, held)
    }
    pub(in crate::node_runtime::pg_store) fn install_in_store(
        &mut self,
        proof: &crate::node_runtime::pg_store::DirectTerminalInstallationProof<'_, '_>,
        pg: &PgStore,
        authority: &EmbeddedPublicationAuthority<'_>,
        request: &CompletePublicationRequest<'_>,
    ) -> Result<(), Error> {
        if self.remote_preparation.is_some() {
            return Err(Error::Window(
                "remote prepared direct terminal requires its original remote section".into(),
            ));
        }
        let deadline = authority.direct_terminal_installation_deadline(proof)?;
        authority.require_effect(pg, request)?;
        let fence = proof.require_captured(self, request)?;
        if self.installation()
            == crate::node_runtime::pg_store::DirectTerminalInstallationObservation::Installed
        {
            return Ok(());
        }
        self.submission
            .install_direct_terminal(
                pg,
                request.node_id().as_u32(),
                request.topology(),
                fence,
                deadline,
                proof,
            )
            .map_err(|e| match e {
                super::super::submission::SubmissionError::Rejected(e)
                | super::super::submission::SubmissionError::Unconfirmed(e) => Error::Store(e),
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

    pub(in crate::node_runtime::pg_store) fn capture(
        permit: &DirectTerminalPreparation<'_, '_>,
        pg: &PgStore,
        authority: &EmbeddedPublicationAuthority<'_>,
        request: &CompletePublicationRequest<'_>,
    ) -> Result<Self, Error> {
        permit.require_authority(pg, authority, request)?;
        let snapshot = pg
            .command_build_snapshot()
            .map_err(|e| Error::Window(e.to_string()))?;
        let command = permit.command_in_store(pg, authority, request)?;
        let index = pg
            .max_metadata_command_log_index(request.id().cluster_epoch())?
            .checked_add(1)
            .and_then(MetadataCommandLogIndex::new)
            .ok_or_else(|| Error::Window("direct terminal command index exhausted".into()))?;
        let command = MetadataCommandEnvelope::new(
            MetadataCommandId::new(request.id().cluster_epoch(), request.id().pg_id(), index),
            MetadataCommandPayload::CommitDirectPutObject(Box::new(command)),
        );
        super::super::object::validate_in_open_transaction(pg, request.topology(), &command)
            .map_err(|e| Error::Window(e.to_string()))?;
        #[cfg(test)]
        let expected_command = command.clone();
        let context = ContextualCommand {
            input: Input::Ordinary(command),
            topology: request.topology().clone(),
        };
        context
            .observe_pending_inventory_in_open_transaction(pg, request.topology())
            .map_err(|e| Error::Window(e.to_string()))?;
        snapshot
            .finish()
            .map_err(|e| Error::Window(e.to_string()))?;
        permit.require_authority(pg, authority, request)?;
        pg.require_live_allocation_lifetime()?;
        Ok(Self {
            submission: context.into_submission(),
            remote_preparation: None,
            #[cfg(test)]
            command: expected_command,
        })
    }
}

impl ContextualCommand {
    #[cfg(test)]
    pub(in crate::node_runtime::pg_store) fn test_assert_direct_displacement(
        &self,
        displaced: bool,
    ) {
        let Input::Ordinary(command) = &self.input else {
            panic!("ordinary")
        };
        let MetadataCommandPayload::CommitDirectPutObject(commit) = command.payload() else {
            panic!("direct")
        };
        assert_eq!(commit.stale_payload.is_some(), displaced);
    }
    #[cfg(test)]
    pub(in crate::node_runtime::pg_store) fn test_cross_direct_terminal_reply(
        self,
        field: &str,
    ) -> Self {
        let Input::Ordinary(command) = self.input else {
            panic!("ordinary")
        };
        let mut id = command.id();
        let MetadataCommandPayload::CommitDirectPutObject(mut commit) = command.payload().clone()
        else {
            panic!("terminal")
        };
        match field {
            "timestamp" => {
                commit.last_modified_millis += 60_000;
                if let Some(reclaim) = &mut commit.stale_payload {
                    match reclaim {
                        crate::metadata_command::ObjectPayloadReclaimCommand::Segments(root) => {
                            root.created_at = commit.last_modified_millis
                        }
                        crate::metadata_command::ObjectPayloadReclaimCommand::Multipart(root) => {
                            root.created_at = commit.last_modified_millis
                        }
                    }
                }
            }
            "future-index" => {
                id = MetadataCommandId::new(
                    id.cluster_epoch(),
                    id.pg_id(),
                    MetadataCommandLogIndex::new(id.log_index().get() + 1).unwrap(),
                )
            }
            "index" => {
                id = MetadataCommandId::new(
                    id.cluster_epoch(),
                    id.pg_id(),
                    MetadataCommandLogIndex::new(1).unwrap(),
                )
            }
            "generation" => {
                commit.object.generation_id =
                    crate::GenerationId::new(commit.object.generation_id.get() + 1).unwrap()
            }
            "key" => commit.object.key = crate::tests::object_key("crossed"),
            "owner" => commit.object.owner = crate::OwnerIdentity::from_principal("crossed"),
            "etag" => commit.object.etag = crate::ObjectEtag::single_part(999),
            "encryption" => {
                commit.object.encryption = crate::tests::committed_metadata_only_test_encryption(
                    1,
                    &commit.object.bucket,
                    &commit.object.key,
                    commit.object.generation_id,
                )
            }
            "segment" => {
                commit.segments[0].segment_vid =
                    crate::GenerationId::new(commit.segments[0].segment_vid.get() + 1).unwrap()
            }
            "witness" => commit.segments[0].shard_integrity = None,
            "reservation" => commit.bucket_write_reservation.owner_token.push('x'),
            "sequence" => commit.write_sequence = 0,
            _ => panic!("unknown fault"),
        }
        Self {
            input: Input::Ordinary(MetadataCommandEnvelope::new(
                id,
                MetadataCommandPayload::CommitDirectPutObject(commit),
            )),
            topology: self.topology,
        }
    }
    pub(super) fn require_direct_terminal_installation(
        &self,
        pg: &PgStore,
    ) -> Result<(), crate::StoreError> {
        let Input::Ordinary(command) = &self.input else {
            return Err(crate::StoreError::RouteCapabilitySubjectMismatch {
                operation: "direct terminal installation context",
            });
        };
        super::super::object::validate_in_open_transaction(pg, &self.topology, command).map_err(
            |_| crate::StoreError::RouteCapabilitySubjectMismatch {
                operation: "direct terminal installation ownership",
            },
        )
    }
}
