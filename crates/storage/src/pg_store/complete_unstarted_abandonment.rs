// Copyright The Argmin Authors.
// SPDX-License-Identifier: Apache-2.0

//! A primary tombstone does not retire pending custody. Replica convergence,
//! follow-up creation and physical cleanup remain separate gated operations.

use super::*;
use crate::node_runtime::pg_store::UnstartedAbandonmentAuthority;
use crate::node_runtime::server::CompleteAbandonmentAuthority;
use crate::storage_rpc::complete_abandonment as wire;

pub(in crate::node_runtime) struct CompleteAbandonmentQuery {
    context: ContextualCommand,
    deadline: PublicationDeadline,
    node: NodeId,
}

pub(in crate::node_runtime) struct CompleteAbandonmentRequest<'a> {
    inner: CompletePublicationRequest<'a>,
}

impl CompleteAbandonmentQuery {
    pub(in crate::node_runtime) fn decode(
        bytes: &[u8],
        topology: &PgTopology,
    ) -> Result<Self, StorageRpcPayloadError> {
        let wire = wire::decode_request(bytes)?;
        // Rebind once, before context decoding; projections cannot refresh it.
        let deadline = PublicationDeadline::received(wire.deadline);
        let context = ContextualCommand::decode_command(
            wire.command.command,
            wire.command.checksum,
            topology,
        )
        .map_err(|_| invalid("complete abandonment command context"))?;
        if context.id().cluster_epoch() != wire.command.epoch
            || context.id().pg_id() != wire.command.pg_id
        {
            return Err(invalid("complete abandonment request subject"));
        }
        Ok(Self {
            context,
            deadline,
            node: wire.command.node_id,
        })
    }

    pub(in crate::node_runtime) fn request(
        &self,
    ) -> Result<CompleteAbandonmentRequest<'_>, StorageRpcPayloadError> {
        Ok(CompleteAbandonmentRequest {
            inner: CompletePublicationRequest {
                command: self
                    .context
                    .encode_command()
                    .map_err(|_| invalid("encode complete abandonment context"))?,
                node_id: self.node,
                work_deadline: &self.deadline,
            },
        })
    }
}

impl CompleteAbandonmentRequest<'_> {
    pub(in crate::node_runtime) fn publication_request(&self) -> &CompletePublicationRequest<'_> {
        &self.inner
    }
    pub(in crate::node_runtime) fn id(&self) -> MetadataCommandId {
        self.inner.id()
    }
    pub(in crate::node_runtime) fn node_id(&self) -> NodeId {
        self.inner.node_id()
    }
    pub(in crate::node_runtime) fn topology(&self) -> &PgTopology {
        self.inner.topology()
    }
    pub(in crate::node_runtime) fn command_sha256(&self) -> [u8; 32] {
        self.inner.command_sha256()
    }
    pub(in crate::node_runtime) fn bound_deadline(
        &self,
        deadline: std::time::Instant,
    ) -> std::time::Instant {
        self.inner.bound_deadline(deadline)
    }

    pub(in crate::node_runtime) fn abandon_response(
        &self,
        pg: &PgStore,
        authority: &CompleteAbandonmentAuthority<'_>,
    ) -> Result<Vec<u8>, CompletePublicationError> {
        self.inner.abandon_unstarted_inner(
            pg,
            AbandonmentEffectAuthority::Remote(authority),
            #[cfg(test)]
            |stage| {
                if stage == "before-tombstone" {
                    crate::node_runtime::server::test_abandonment_before_insert(self);
                }
                Ok(())
            },
        )?;
        Ok(wire::encode_confirmation(wire::Confirmation {
            node_id: self.node_id(),
            id: self.id(),
            command_sha256: self.command_sha256(),
        }))
    }

    #[cfg(test)]
    pub(in crate::node_runtime) fn test_abandon_with_hook(
        &self,
        pg: &PgStore,
        authority: &CompleteAbandonmentAuthority<'_>,
        hook: impl FnMut(&'static str) -> Result<(), StoreError>,
    ) -> Result<(), CompletePublicationError> {
        self.inner
            .abandon_unstarted_inner(pg, AbandonmentEffectAuthority::Remote(authority), hook)
    }
}

enum AbandonmentEffectAuthority<'a> {
    Embedded(
        &'a EmbeddedPublicationAuthority<'a>,
        &'a UnstartedAbandonmentAuthority<'a, 'a>,
    ),
    Remote(&'a CompleteAbandonmentAuthority<'a>),
}

impl AbandonmentEffectAuthority<'_> {
    fn require_effect(
        &self,
        pg: &PgStore,
        request: &CompletePublicationRequest<'_>,
    ) -> Result<(), StoreError> {
        match self {
            Self::Embedded(authority, custody) => {
                custody.require_request(request)?;
                authority.require_effect(pg, request)
            }
            Self::Remote(authority) => authority.require_effect(pg, request),
        }
    }

    fn record<'a>(&'a self, request: &'a CompletePublicationRequest<'a>) -> AbandonmentRecord<'a> {
        match self {
            Self::Embedded(authority, custody) => AbandonmentRecord::Complete {
                command: &request.command,
                request,
                authority,
                custody,
            },
            Self::Remote(authority) => AbandonmentRecord::RemoteComplete {
                command: &request.command,
                request,
                authority,
            },
        }
    }
}

impl CompletePublicationRequest<'_> {
    pub(in crate::node_runtime) fn abandon_unstarted_embedded(
        &self,
        pg: &PgStore,
        authority: &EmbeddedPublicationAuthority<'_>,
        custody: &UnstartedAbandonmentAuthority<'_, '_>,
        #[cfg(test)] hook: impl FnMut(&'static str) -> Result<(), StoreError>,
    ) -> Result<(), CompletePublicationError> {
        self.abandon_unstarted_inner(
            pg,
            AbandonmentEffectAuthority::Embedded(authority, custody),
            #[cfg(test)]
            hook,
        )
    }

    fn abandon_unstarted_inner(
        &self,
        pg: &PgStore,
        authority: AbandonmentEffectAuthority<'_>,
        #[cfg(test)] mut hook: impl FnMut(&'static str) -> Result<(), StoreError>,
    ) -> Result<(), CompletePublicationError> {
        let require_effect = || authority.require_effect(pg, self);
        pg.require_usable_connection()?;
        require_effect()?;
        if !pg.conn.is_autocommit() {
            return Err(StoreError::RouteCapabilitySubjectMismatch {
                operation: "complete abandonment inside caller transaction",
            }
            .into());
        }
        let transaction = MetadataTransaction::begin(pg, "complete abandonment begin")?;
        let result = (|| -> Result<(), Failure> {
            let command = &self.command;
            let node = self.node_id().as_u32();
            let identity = TerminalCommandIdentity::complete(command);
            command.require_retained_pending_indexes(pg, node, self.topology())?;
            let slot = pg
                .pending_metadata_command_slot_with_validator(node, |bytes, crc, id| {
                    if command.validate_slot_bytes(bytes, crc, id) {
                        Ok(())
                    } else {
                        Err(identity.conflict(node, pg.pg_id))
                    }
                })?
                .ok_or_else(|| identity.conflict(node, pg.pg_id))?;
            if slot.publication_started
                || slot.scope_bucket.as_ref() != Some(command.scope_bucket())
            {
                return Err(identity.conflict(node, pg.pg_id).into());
            }
            let abandoned =
                match pg.metadata_command_identity_startup_disposition(node, &identity)? {
                    MetadataCommandStartupDisposition::Absent => false,
                    MetadataCommandStartupDisposition::Abandoned { .. } => true,
                    MetadataCommandStartupDisposition::Applied { .. } => {
                        return Err(identity.conflict(node, pg.pg_id).into());
                    }
                };
            require_effect()?;
            if !abandoned {
                pg.record_metadata_command_abandoned_inner(
                    node,
                    &authority.record(self),
                    #[cfg(test)]
                    || hook("before-tombstone"),
                )?;
            }
            #[cfg(test)]
            hook("before-commit")?;
            require_effect()?;
            Ok(())
        })();
        finish_transaction(transaction, result, "complete abandonment commit")?;
        pg.invalidate_clean_metadata_digest_revision();
        #[cfg(test)]
        hook("after-commit")?;
        Ok(())
    }
}
