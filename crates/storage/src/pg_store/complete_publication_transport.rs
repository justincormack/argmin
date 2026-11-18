// Copyright The Argmin Authors.
// SPDX-License-Identifier: Apache-2.0

//! Candidate payload integration, selected by owner-local installed RPC tests.
//! Production dispatch is not switched. The sender freezes
//! its conservative wall cutoff once; the receiver binds it once, retaining it
//! in every borrowed request. Confirmation is marker evidence, never apply,
//! retirement, release or cleanup authority. Any error leaves custody unresolved.

use super::*;
use crate::storage_rpc::{complete_publication as wire, StorageRpcOperationDeadline};
use std::time::Instant;

pub(in crate::node_runtime) struct CompletePublicationTransport<'a> {
    request: &'a CompletePublicationRequest<'a>,
    deadline: Instant,
    portable: StorageRpcOperationDeadline,
}

pub(in crate::node_runtime) struct CompletePublicationConfirmation<'a> {
    transport: &'a CompletePublicationTransport<'a>,
}

impl CompletePublicationConfirmation<'_> {
    pub(in crate::node_runtime) fn id(&self) -> MetadataCommandId {
        self.transport.request.id()
    }
}

impl CompletePublicationRequest<'_> {
    pub(in crate::node_runtime) fn prepare_transport(
        &self,
        deadline: Instant,
    ) -> CompletePublicationTransport<'_> {
        let captured = self.work_deadline.capture(deadline);
        let deadline = deadline.min(captured.local);
        let mut portable = captured.portable;
        if deadline < captured.local {
            // An individual attempt may shorten its budget, but cannot replace
            // the holder's first cutoff. Equal/later budgets reuse it verbatim,
            // including when request views are dropped and recreated.
            portable.portable_wall_valid_until_ms = portable.portable_wall_valid_until_ms.min(
                StorageRpcOperationDeadline::from_instant(deadline).portable_wall_valid_until_ms,
            );
        }
        CompletePublicationTransport {
            request: self,
            deadline,
            portable,
        }
    }

    /// The only typed producer of a successful marker acknowledgement. Encoding
    /// is fixed-size/infallible and occurs only after the admitted store commit.
    pub(in crate::node_runtime) fn start_response(
        &self,
        pg: &PgStore,
        authority: &CompletePublicationAuthority<'_>,
    ) -> Result<Vec<u8>, CompletePublicationError> {
        self.start(pg, authority)?;
        Ok(wire::encode_confirmation(wire::Confirmation {
            node_id: self.node_id(),
            id: self.id(),
            command_sha256: self.command_sha256(),
        }))
    }
}

impl CompletePublicationTransport<'_> {
    pub(in crate::node_runtime::pg_store) fn encode_replica_abandonment_request(
        &self,
        attempt: &crate::node_runtime::pg_store::CompleteReplicaAbandonmentAttempt<'_>,
    ) -> Result<Vec<u8>, CompletePendingInspectionError> {
        attempt.require_transport(self)?;
        attempt.require_request(self.request)?;
        if Instant::now() >= self.deadline {
            return Err(StoreError::OperationDeadlineExceeded {
                context: "encode replica abandonment request",
            }
            .into());
        }
        Ok(
            crate::storage_rpc::complete_replica_abandonment::encode_request(
                crate::storage_rpc::complete_replica_abandonment::BorrowedRequest {
                    primary: attempt.primary(),
                    command: framing::BorrowedRequest {
                        node_id: self.request.node_id(),
                        epoch: self.request.id().cluster_epoch(),
                        pg_id: self.request.id().pg_id(),
                        checksum: self.request.command.checksum_crc64(),
                        command: self.request.command.bytes(),
                    },
                    deadline: self.portable,
                },
            )?,
        )
    }

    pub(in crate::node_runtime::pg_store) fn decode_replica_abandonment_confirmation(
        &self,
        primary: NodeId,
        bytes: &[u8],
    ) -> Result<(), StorageRpcPayloadError> {
        crate::storage_rpc::complete_replica_abandonment::decode_confirmation_for(
            bytes,
            primary,
            self.request.node_id(),
            self.request.id(),
            self.request.command_sha256(),
        )
    }

    pub(in crate::node_runtime::pg_store) fn encode_abandonment_request(
        &self,
        custody: &crate::node_runtime::pg_store::UnstartedAbandonmentAuthority<'_, '_>,
    ) -> Result<Vec<u8>, CompletePendingInspectionError> {
        custody.require_request(self.request)?;
        if Instant::now() >= self.deadline {
            return Err(StoreError::OperationDeadlineExceeded {
                context: "encode complete abandonment request",
            }
            .into());
        }
        Ok(crate::storage_rpc::complete_abandonment::encode_request(
            crate::storage_rpc::complete_abandonment::BorrowedRequest {
                command: framing::BorrowedRequest {
                    node_id: self.request.node_id(),
                    epoch: self.request.id().cluster_epoch(),
                    pg_id: self.request.id().pg_id(),
                    checksum: self.request.command.checksum_crc64(),
                    command: self.request.command.bytes(),
                },
                deadline: self.portable,
            },
        )?)
    }

    pub(in crate::node_runtime::pg_store) fn decode_abandonment_confirmation(
        &self,
        bytes: &[u8],
    ) -> Result<(), StorageRpcPayloadError> {
        crate::storage_rpc::complete_abandonment::decode_confirmation_for(
            bytes,
            self.request.node_id(),
            self.request.id(),
            self.request.command_sha256(),
        )
    }

    pub(in super::super::super) fn require_fanout_route(
        &self,
        route: &crate::cluster::CompleteFanoutRoute<'_>,
    ) -> Result<(), StoreError> {
        route.require_command(
            self.request.id(),
            self.request.checksum_crc64(),
            self.request.command_sha256(),
        )
    }

    // Closed borrowed operation: neither the command nor a fresh submission
    // escapes the terminal holder, and the attempt retains its original cutoff.
    pub(in crate::node_runtime::pg_store) fn retire_embedded(
        &self,
        section: &crate::cluster::EmbeddedPublicationSection<'_>,
        terminal: &crate::node_runtime::pg_store::TerminalSlotRetirementAuthority<'_>,
        #[cfg(test)] hook: impl FnMut(&'static str) -> Result<(), StoreError>,
    ) -> Result<(), CompletePublicationError> {
        section.retire_complete_publication(
            self.request,
            terminal,
            self.deadline,
            #[cfg(test)]
            hook,
        )
    }

    // Internal borrowed bridge to the application request owner. The owning
    // sender never exposes this transport or its command to its caller.
    pub(in crate::node_runtime::pg_store::command_log) fn application_command(
        &self,
    ) -> &EncodedContextualCommand<'_> {
        &self.request.command
    }

    // Requires the stack-only permit derived by the complete terminal holder;
    // ordinary pending/publication callers cannot mint retirement bytes.
    pub(in crate::node_runtime::pg_store) fn encode_retirement_request(
        &self,
        terminal: &crate::node_runtime::pg_store::TerminalSlotRetirementAuthority<'_>,
    ) -> Result<Vec<u8>, CompletePendingInspectionError> {
        terminal.require_request(self.request)?;
        if Instant::now() >= self.deadline {
            return Err(StoreError::OperationDeadlineExceeded {
                context: "encode complete retirement request",
            }
            .into());
        }
        Ok(crate::storage_rpc::complete_retirement::encode_request(
            crate::storage_rpc::complete_retirement::BorrowedRequest {
                command: framing::BorrowedRequest {
                    node_id: self.request.node_id(),
                    epoch: self.request.id().cluster_epoch(),
                    pg_id: self.request.id().pg_id(),
                    checksum: self.request.command.checksum_crc64(),
                    command: self.request.command.bytes(),
                },
                deadline: self.portable,
            },
        )?)
    }

    pub(in crate::node_runtime::pg_store) fn decode_retirement_confirmation(
        &self,
        bytes: &[u8],
    ) -> Result<(), StorageRpcPayloadError> {
        crate::storage_rpc::complete_retirement::decode_confirmation_for(
            bytes,
            self.request.node_id(),
            self.request.id(),
            self.request.command_sha256(),
        )
    }

    // Only the owning pending/publication subtree can derive application bytes
    // from this transport. Ordinary callers must borrow an owning attempt.
    pub(in super::super::super) fn encode_application_request(
        &self,
    ) -> Result<Vec<u8>, CompletePendingInspectionError> {
        if Instant::now() >= self.deadline {
            return Err(StoreError::OperationDeadlineExceeded {
                context: "encode complete application request",
            }
            .into());
        }
        Ok(crate::storage_rpc::complete_application::encode_request(
            crate::storage_rpc::complete_application::BorrowedRequest {
                command: framing::BorrowedRequest {
                    node_id: self.request.node_id(),
                    epoch: self.request.id().cluster_epoch(),
                    pg_id: self.request.id().pg_id(),
                    checksum: self.request.command.checksum_crc64(),
                    command: self.request.command.bytes(),
                },
                deadline: self.portable,
            },
        )?)
    }

    pub(in super::super::super) fn decode_application_confirmation(
        &self,
        bytes: &[u8],
    ) -> Result<crate::metadata_command::MetadataCommandReplicaState, StorageRpcPayloadError> {
        crate::storage_rpc::complete_application::decode_confirmation_for(
            bytes,
            self.request.node_id(),
            self.request.id(),
            self.request.command_sha256(),
        )
    }

    pub(in crate::node_runtime) fn node_id(&self) -> NodeId {
        self.request.node_id()
    }

    pub(in crate::node_runtime) fn id(&self) -> MetadataCommandId {
        self.request.id()
    }

    pub(in crate::node_runtime) fn deadline(&self) -> Instant {
        self.deadline
    }

    pub(in crate::node_runtime) fn encode_request(
        &self,
    ) -> Result<Vec<u8>, CompletePendingInspectionError> {
        if Instant::now() >= self.deadline {
            return Err(StoreError::OperationDeadlineExceeded {
                context: "encode complete publication request",
            }
            .into());
        }
        Ok(wire::encode_request(wire::BorrowedRequest {
            command: framing::BorrowedRequest {
                node_id: self.request.node_id(),
                epoch: self.request.id().cluster_epoch(),
                pg_id: self.request.id().pg_id(),
                checksum: self.request.command.checksum_crc64(),
                command: self.request.command.bytes(),
            },
            deadline: self.portable,
        })?)
    }

    pub(in crate::node_runtime) fn decode_confirmation(
        &self,
        bytes: &[u8],
    ) -> Result<CompletePublicationConfirmation<'_>, StorageRpcPayloadError> {
        let confirmation = wire::decode_confirmation(bytes)?;
        if confirmation.node_id != self.request.node_id()
            || confirmation.id != self.request.id()
            || confirmation.command_sha256 != self.request.command_sha256()
        {
            return Err(invalid("complete publication confirmation subject"));
        }
        // Even a late exact reply may resolve marker uncertainty. It does not
        // extend the request deadline or permit further effects.
        Ok(CompletePublicationConfirmation { transport: self })
    }
}

pub(in crate::node_runtime) struct CompletePublicationQuery {
    node_id: NodeId,
    context: ContextualCommand,
    deadline: PublicationDeadline,
}

impl CompletePublicationQuery {
    pub(in crate::node_runtime) fn deadline(&self) -> Instant {
        self.deadline
            .0
            .get()
            .expect("decoded deadline bound once")
            .local
    }

    pub(in crate::node_runtime) fn decode(
        bytes: &[u8],
        topology: &PgTopology,
    ) -> Result<Self, StorageRpcPayloadError> {
        let wire = wire::decode_request(bytes)?;
        // Bind before potentially expensive command/context validation. Neither
        // reinspection nor repeated request projection restarts remaining time.
        let deadline = PublicationDeadline::received(wire.deadline);
        let context = ContextualCommand::decode_command(
            wire.command.command,
            wire.command.checksum,
            topology,
        )
        .map_err(|_| invalid("complete publication command context"))?;
        if context.id().cluster_epoch() != wire.command.epoch
            || context.id().pg_id() != wire.command.pg_id
        {
            return Err(invalid("complete publication request subject"));
        }
        Ok(Self {
            node_id: wire.command.node_id,
            context,
            deadline,
        })
    }

    pub(in crate::node_runtime) fn request(
        &self,
    ) -> Result<CompletePublicationRequest<'_>, StorageRpcPayloadError> {
        Ok(CompletePublicationRequest {
            node_id: self.node_id,
            command: self
                .context
                .encode_command()
                .map_err(|_| invalid("encode complete publication context"))?,
            work_deadline: &self.deadline,
        })
    }
}
