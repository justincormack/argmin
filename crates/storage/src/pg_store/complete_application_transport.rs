// Copyright The Argmin Authors.
// SPDX-License-Identifier: Apache-2.0

//! Receiver-owned complete input and immutable deadline. No sender custody or
//! installation inference; an application error never establishes nonacceptance.

use super::*;
use crate::node_runtime::pg_store::payload_staging::ContextualCommand;
use crate::storage_rpc::{complete_application as wire, StorageRpcPayloadError};

pub(in crate::node_runtime) struct CompleteApplicationQuery {
    node_id: NodeId,
    context: ContextualCommand,
    deadline: Instant,
}

impl CompleteApplicationQuery {
    pub(in crate::node_runtime) fn decode(
        bytes: &[u8],
        topology: &PgTopology,
    ) -> Result<Self, StorageRpcPayloadError> {
        let wire = wire::decode_request(bytes)?;
        // Conservative absolute wall cutoff -> receiver monotonic, before any
        // expensive context work. Projection never resamples or extends it.
        let deadline = wire.deadline.local_deadline();
        let context = ContextualCommand::decode_command(
            wire.command.command,
            wire.command.checksum,
            topology,
        )
        .map_err(|_| {
            StorageRpcPayloadError::InvalidResponseEnvelope("complete application command context")
        })?;
        if context.id().cluster_epoch() != wire.command.epoch
            || context.id().pg_id() != wire.command.pg_id
        {
            return Err(StorageRpcPayloadError::InvalidResponseEnvelope(
                "complete application request subject",
            ));
        }
        Ok(Self {
            node_id: wire.command.node_id,
            context,
            deadline,
        })
    }

    pub(in crate::node_runtime) fn deadline(&self) -> Instant {
        self.deadline
    }

    /// Lends only the opaque view, never the context or independently installable
    /// bytes. The operation budget may shorten, but cannot renew, the wire cutoff.
    pub(in crate::node_runtime) fn with_request<T>(
        &self,
        operation_deadline: Instant,
        action: impl FnOnce(&CompleteApplicationRequest<'_>) -> T,
    ) -> Result<T, StorageRpcPayloadError> {
        let encoded = self.context.encode_command().map_err(|_| {
            StorageRpcPayloadError::InvalidResponseEnvelope("encode complete application context")
        })?;
        let request = CompleteApplicationRequest {
            command: &encoded,
            node_id: self.node_id,
            deadline: self.deadline.min(operation_deadline),
        };
        Ok(action(&request))
    }
}
