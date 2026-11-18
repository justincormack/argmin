// Copyright The Argmin Authors.
// SPDX-License-Identifier: Apache-2.0

use super::*;
use crate::storage_rpc::complete_replica_abandonment as wire;

pub(in crate::node_runtime) struct CompleteReplicaAbandonmentQuery {
    context: ContextualCommand,
    deadline: PublicationDeadline,
    primary: NodeId,
    node: NodeId,
}

pub(in crate::node_runtime) struct CompleteReplicaAbandonmentRequest<'a> {
    inner: CompletePublicationRequest<'a>,
    primary: NodeId,
}

impl CompleteReplicaAbandonmentQuery {
    pub(in crate::node_runtime) fn decode(
        bytes: &[u8],
        topology: &PgTopology,
    ) -> Result<Self, StorageRpcPayloadError> {
        let wire = wire::decode_request(bytes)?;
        // Network transit cannot restart authority. Bind before context parsing.
        let deadline = PublicationDeadline::received(wire.deadline);
        let context = ContextualCommand::decode_command(
            wire.command.command,
            wire.command.checksum,
            topology,
        )
        .map_err(|_| invalid("replica abandonment command context"))?;
        if context.id().cluster_epoch() != wire.command.epoch
            || context.id().pg_id() != wire.command.pg_id
        {
            return Err(invalid("replica abandonment command subject"));
        }
        Ok(Self {
            context,
            deadline,
            primary: wire.primary,
            node: wire.command.node_id,
        })
    }

    pub(in crate::node_runtime) fn request(
        &self,
    ) -> Result<CompleteReplicaAbandonmentRequest<'_>, StorageRpcPayloadError> {
        Ok(CompleteReplicaAbandonmentRequest {
            inner: CompletePublicationRequest {
                command: self
                    .context
                    .encode_command()
                    .map_err(|_| invalid("encode replica abandonment context"))?,
                node_id: self.node,
                work_deadline: &self.deadline,
            },
            primary: self.primary,
        })
    }
}

impl CompleteReplicaAbandonmentRequest<'_> {
    pub(in crate::node_runtime) fn primary(&self) -> NodeId {
        self.primary
    }

    pub(in crate::node_runtime) fn publication_request(&self) -> &CompletePublicationRequest<'_> {
        &self.inner
    }

    pub(in crate::node_runtime) fn abandon_response(
        &self,
        pg: &PgStore,
        authority: &RemoteReplicaAbandonmentAuthority<'_>,
    ) -> Result<Vec<u8>, CompletePublicationError> {
        authority.require_received_request(pg, self)?;
        pg.record_remote_complete_replica_abandonment(
            authority,
            #[cfg(test)]
            |stage| {
                crate::node_runtime::server::test_replica_abandonment_stage(stage);
                Ok(())
            },
        )?;
        Ok(wire::encode_confirmation(
            self.primary,
            crate::storage_rpc::complete_abandonment::Confirmation {
                node_id: self.inner.node_id(),
                id: self.inner.id(),
                command_sha256: self.inner.command_sha256(),
            },
        ))
    }
}
