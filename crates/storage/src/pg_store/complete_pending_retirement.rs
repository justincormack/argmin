// Copyright The Argmin Authors.
// SPDX-License-Identifier: Apache-2.0

//! Exact complete-slot retirement. The local prerequisite requires terminal
//! custody and admitted primary authority; the closed transport's receiver has
//! a separate admitted capability over its eligible complete context. Neither
//! entry retires owners, releases resources or deletes payload.

use super::*;
use crate::node_runtime::pg_store::command_log::complete_settled_prefix::{
    PrefixTransition, RetirementEffectAuthority,
};
use crate::node_runtime::pg_store::TerminalSlotRetirementAuthority;
use crate::node_runtime::server::CompleteRetirementAuthority;
use crate::storage_rpc::complete_retirement as wire;

/// Decoding does not grant removal authority. Only the server's private admitted
/// owner can supply the independent effect capability required below.
pub(in crate::node_runtime) struct CompleteRetirementQuery {
    context: ContextualCommand,
    deadline: PublicationDeadline,
    node: NodeId,
}

pub(in crate::node_runtime) struct CompleteRetirementRequest<'a> {
    inner: CompletePublicationRequest<'a>,
}

impl CompleteRetirementQuery {
    pub(in crate::node_runtime) fn decode(
        bytes: &[u8],
        topology: &PgTopology,
    ) -> Result<Self, StorageRpcPayloadError> {
        let wire = wire::decode_request(bytes)?;
        let deadline = PublicationDeadline::received(wire.deadline);
        let context = ContextualCommand::decode_command(
            wire.command.command,
            wire.command.checksum,
            topology,
        )
        .map_err(|_| invalid("complete retirement command context"))?;
        if context.id().cluster_epoch() != wire.command.epoch
            || context.id().pg_id() != wire.command.pg_id
        {
            return Err(invalid("complete retirement request subject"));
        }
        if context.terminal_slot_work()
            == crate::node_runtime::pg_store::payload_staging::TerminalSlotWork::Gated
        {
            return Err(invalid(
                "complete retirement still requires terminal work integration",
            ));
        }
        Ok(Self {
            context,
            deadline,
            node: wire.command.node_id,
        })
    }

    pub(in crate::node_runtime) fn request(
        &self,
    ) -> Result<CompleteRetirementRequest<'_>, StorageRpcPayloadError> {
        Ok(CompleteRetirementRequest {
            inner: CompletePublicationRequest {
                command: self
                    .context
                    .encode_command()
                    .map_err(|_| invalid("encode complete retirement context"))?,
                node_id: self.node,
                work_deadline: &self.deadline,
            },
        })
    }
}

impl CompleteRetirementRequest<'_> {
    // Borrowed only; retirement still requires its separate admitted authority.
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

    pub(in crate::node_runtime) fn retire_response(
        &self,
        pg: &PgStore,
        authority: &CompleteRetirementAuthority<'_>,
    ) -> Result<Vec<u8>, CompletePublicationError> {
        self.inner.retire_inner(
            pg,
            RetirementEffectAuthority::Remote(authority),
            #[cfg(test)]
            |stage| {
                if stage == "before-delete" {
                    crate::node_runtime::server::test_retirement_before_delete(self);
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
    pub(in crate::node_runtime) fn test_retire_with_hook(
        &self,
        pg: &PgStore,
        authority: &CompleteRetirementAuthority<'_>,
        hook: impl FnMut(&'static str) -> Result<(), StoreError>,
    ) -> Result<(), CompletePublicationError> {
        self.inner
            .retire_inner(pg, RetirementEffectAuthority::Remote(authority), hook)
    }
}

impl CompletePublicationRequest<'_> {
    #[cfg(test)]
    pub(in crate::node_runtime::pg_store) fn retire_quiescent_pending(
        &self,
        pg: &PgStore,
        authority: &CompletePublicationAuthority<'_>,
        terminal: &TerminalSlotRetirementAuthority<'_>,
        #[cfg(test)] hook: impl FnMut(&'static str) -> Result<(), StoreError>,
    ) -> Result<(), CompletePublicationError> {
        self.retire_inner(
            pg,
            RetirementEffectAuthority::Local(authority, terminal),
            #[cfg(test)]
            hook,
        )
    }

    pub(in crate::node_runtime) fn retire_embedded(
        &self,
        pg: &PgStore,
        authority: &crate::node_runtime::engine::EmbeddedPublicationAuthority<'_>,
        terminal: &TerminalSlotRetirementAuthority<'_>,
        #[cfg(test)] hook: impl FnMut(&'static str) -> Result<(), StoreError>,
    ) -> Result<(), CompletePublicationError> {
        self.retire_inner(
            pg,
            RetirementEffectAuthority::Embedded(authority, terminal),
            #[cfg(test)]
            hook,
        )
    }

    pub(in crate::node_runtime::pg_store::command_log) fn retire_effects(
        &self,
        pg: &PgStore,
        authority: RetirementEffectAuthority<'_>,
        prefix: &mut PrefixTransition<'_>,
        #[cfg(test)] mut hook: impl FnMut(&'static str) -> Result<(), StoreError>,
    ) -> Result<(), CompletePublicationError> {
        let require_effect = || authority.require_effect(pg, self);
        pg.require_usable_connection()?;
        require_effect()?;
        if !pg.conn.is_autocommit() {
            return Err(StoreError::RouteCapabilitySubjectMismatch {
                operation: "complete slot retirement inside caller transaction",
            }
            .into());
        }
        let transaction = MetadataTransaction::begin(pg, "complete slot retirement begin")?;
        let result = (|| -> Result<(), Failure> {
            let command = &self.command;
            let node = self.node_id().as_u32();
            let identity = TerminalCommandIdentity::complete(command);
            let state = pg.metadata_command_replica_state()?;
            if state.cluster_epoch != self.id().cluster_epoch()
                || state.applied_log_index < self.id().log_index().get()
                || !matches!(
                    pg.metadata_command_identity_startup_disposition(node, &identity)?,
                    MetadataCommandStartupDisposition::Applied { .. }
                )
            {
                return Err(identity.conflict(node, pg.pg_id).into());
            }
            let slot =
                pg.pending_metadata_command_slot_with_validator(node, |bytes, crc, id| {
                    if id == command.id()
                        && crc == command.checksum_crc64()
                        && bytes == command.bytes()
                    {
                        Ok(())
                    } else {
                        Err(identity.conflict(node, pg.pg_id))
                    }
                })?;
            if let Some(slot) = &slot {
                if !slot.publication_started
                    || slot.scope_bucket.as_ref() != Some(command.scope_bucket())
                {
                    return Err(identity.conflict(node, pg.pg_id).into());
                }
                command.require_retained_pending_indexes(pg, node, self.topology())?;
            } else {
                // Absence can confirm a lost reply, but not orphaned sidecars.
                // The FK cascades remove all of these in the same transaction.
                for table in [
                    "metadata_command_pending_route_dependencies",
                    "metadata_command_pending_staging_protection",
                    "metadata_command_pending_placed_reference_pages",
                    "metadata_command_pending_reclaim_reference_pages",
                ] {
                    let present: bool = pg
                        .conn
                        .query_row(
                            &format!("SELECT EXISTS(SELECT 1 FROM {table})"),
                            [],
                            |row| row.get(0),
                        )
                        .map_err(|error| {
                            database("complete slot retirement orphan check", error)
                        })?;
                    if present {
                        return Err(identity.conflict(node, pg.pg_id).into());
                    }
                }
            }
            // A released bucket proof is not transported as authority. The
            // receiver independently requires its exact durable retry owner in
            // this same transaction, including on absence/lost-reply retries.
            command.require_slot_retirement_custody(pg)?;
            let mut removal = if slot.is_some() {
                Some(pg.conn.prepare_cached("DELETE FROM metadata_command_pending_slot WHERE singleton = 0 AND cluster_epoch = ?1 AND pg_id = ?2 AND log_index = ?3 AND command_checksum = ?4 AND command_bytes = ?5 AND scope_bucket = ?6 AND publication_started = 1")
                    .map_err(|error| database("prepare exact complete pending retirement", error))?)
            } else {
                None
            };
            // Preparation can also stall. Check both captured authorities after
            // it, immediately before the first write (or absence confirmation).
            #[cfg(test)]
            hook("before-delete")?;
            prefix.before_effect()?;
            require_effect()?;
            if let Some(statement) = removal.as_mut() {
                let changed = statement
                    .execute(params![
                        self.id().cluster_epoch().get() as i64,
                        self.id().pg_id().get(),
                        self.id().log_index().get() as i64,
                        command.checksum_crc64() as i64,
                        command.bytes(),
                        command.scope_bucket()
                    ])
                    .map_err(|error| database("retire exact complete pending slot", error))?;
                if changed != 1 {
                    return Err(identity.conflict(node, pg.pg_id).into());
                }
            }
            drop(removal);
            prefix.after_effect()?;
            #[cfg(test)]
            hook("before-commit")?;
            require_effect()?;
            Ok(())
        })();
        finish_transaction(transaction, result, "complete slot retirement commit")?;
        #[cfg(test)]
        hook("after-commit")?;
        Ok(())
    }
}
