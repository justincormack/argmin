// Copyright The Argmin Authors.
// SPDX-License-Identifier: Apache-2.0

//! Trial complete application and exact publication marker in one transaction.
//! All errors retain uncertain publication custody; none licenses abandonment
//! or physical cleanup. Current production publication is not selected here.

use super::*;
use crate::node_runtime::engine::EmbeddedPublicationAuthority;
use crate::node_runtime::pg_store::command_log::complete_settled_prefix::{
    PrefixTransition, PublicationAuthority,
};
use crate::node_runtime::pg_store::metadata_transaction::MetadataTransaction;
use crate::node_runtime::pg_store::payload_staging::ApplicationError;
use crate::node_runtime::server::CompletePublicationAuthority;
pub(in crate::node_runtime) use replica_abandonment::{
    CompleteReplicaAbandonmentQuery, CompleteReplicaAbandonmentRequest,
};

#[path = "complete_replica_abandonment.rs"]
mod replica_abandonment;

#[path = "complete_unstarted_abandonment.rs"]
mod abandonment;
pub(in crate::node_runtime) use abandonment::{
    CompleteAbandonmentQuery, CompleteAbandonmentRequest,
};

#[path = "complete_part_cleanup_installation.rs"]
mod part_cleanup_installation;
#[path = "complete_pending_retirement.rs"]
mod retirement;
pub(in crate::node_runtime) use retirement::{CompleteRetirementQuery, CompleteRetirementRequest};

#[derive(Debug, thiserror::Error)]
enum Failure {
    #[error(transparent)]
    RetirementCustody(#[from] crate::node_runtime::pg_store::payload_staging::ContextError),
    #[error(transparent)]
    PartCleanup(#[from] crate::node_runtime::pg_store::AbandonedPartPreparationError),
    #[error(transparent)]
    Store(#[from] StoreError),
    #[error(transparent)]
    Application(#[from] ApplicationError),
    #[error("complete primary transition failed: {cause}; rollback failed: {rollback}")]
    Rollback {
        cause: Box<Self>,
        rollback: StoreError,
    },
}

#[derive(Debug, thiserror::Error)]
#[error(transparent)]
pub(in crate::node_runtime) struct CompletePublicationError(Failure);

impl From<StoreError> for CompletePublicationError {
    fn from(error: StoreError) -> Self {
        Self(Failure::Store(error))
    }
}

impl CompletePublicationError {
    /// Preserve typed storage diagnostics without granting definitive-rejection
    /// semantics to any publication failure, including failed COMMIT/rollback.
    pub(in crate::node_runtime) fn into_store_error(self) -> Result<StoreError, Self> {
        match self.0 {
            Failure::Store(error) => Ok(error),
            other => Err(Self(other)),
        }
    }
}

pub(in crate::node_runtime) struct CompletePublicationRequest<'a> {
    command: EncodedContextualCommand<'a>,
    node_id: NodeId,
    work_deadline: &'a PublicationDeadline,
}

#[path = "complete_publication_transport.rs"]
mod transport;
pub(in crate::node_runtime) use transport::CompletePublicationQuery;
pub(in crate::node_runtime) use transport::CompletePublicationTransport;

impl CompletePendingEvidence {
    pub(in crate::node_runtime) fn publication_request(
        &self,
        node_id: NodeId,
        topology: &PgTopology,
    ) -> Result<CompletePublicationRequest<'_>, StorageRpcPayloadError> {
        let probe = self.applied_probe(node_id, topology)?;
        Ok(CompletePublicationRequest {
            command: probe.command,
            node_id,
            work_deadline: &self.publication_deadline,
        })
    }
}

impl PublicationDeadline {
    // Private to the pending/publication owner subtree. Callers cannot use a
    // submitted context with an independently fabricated/reset deadline.
    pub(in super::super) fn submitted_request<'a>(
        &'a self,
        source: &'a crate::node_runtime::pg_store::payload_staging::SubmittedPublicationEvidence,
        node_id: NodeId,
    ) -> Result<CompletePublicationRequest<'a>, StorageRpcPayloadError> {
        let command = source
            .encode()
            .map_err(|_| invalid("encode submitted publication context"))?;
        Ok(CompletePublicationRequest {
            command,
            node_id,
            work_deadline: self,
        })
    }
}

impl Failure {
    fn rollback(self, transaction: MetadataTransaction<'_>) -> Self {
        match transaction.rollback() {
            Ok(()) => self,
            Err(rollback) => Self::Rollback {
                cause: Box::new(self),
                rollback,
            },
        }
    }
}

// Complete the owned SQL scope before any reply-loss hook. A post-commit
// failure is still uncertain sender custody, never a reason to roll back.
fn finish_transaction(
    transaction: MetadataTransaction<'_>,
    result: Result<(), Failure>,
    commit_context: &'static str,
) -> Result<(), CompletePublicationError> {
    match result {
        Ok(()) => transaction.commit(commit_context).map_err(Into::into),
        Err(error) => Err(CompletePublicationError(error.rollback(transaction))),
    }
}

fn database(context: &'static str, source: rusqlite::Error) -> Failure {
    StoreError::Db {
        context,
        source: source.into(),
    }
    .into()
}

impl CompletePublicationRequest<'_> {
    /// Derive only this owning pair's cleanup and borrow the source's already
    /// captured deadline. No fresh clock projection or independent context input.
    pub(in crate::node_runtime) fn abandoned_part_cleanup_request<'a>(
        permit: &'a crate::node_runtime::pg_store::AbandonedPartPublication<'_, '_>,
    ) -> Result<CompletePublicationRequest<'a>, StoreError> {
        let original = permit.original_request()?;
        Ok(CompletePublicationRequest {
            command: permit.cleanup()?,
            node_id: original.node_id,
            work_deadline: original.work_deadline,
        })
    }

    pub(in crate::node_runtime) fn require_abandoned_part_binding(
        &self,
        permit: &crate::node_runtime::pg_store::AbandonedPartPublication<'_, '_>,
    ) -> Result<(), StoreError> {
        let original = permit.original_request()?;
        let cleanup = permit.cleanup()?;
        if self.id() != cleanup.id()
            || self.command_sha256() != cleanup.command_sha256()
            || self.topology() != cleanup.topology()
            || self.node_id != original.node_id
            || !std::ptr::eq(self.work_deadline, original.work_deadline)
        {
            return Err(StoreError::RouteCapabilitySubjectMismatch {
                operation: "abandoned part publication exact cleanup/primary/deadline owner",
            });
        }
        Ok(())
    }

    /// Only a borrowed permit from the original pair can select a member. The
    /// complete cleanup and the already-frozen deadline stay borrowed from it.
    pub(in crate::node_runtime::pg_store) fn abandoned_part_member_request<'a>(
        permit: &'a crate::node_runtime::pg_store::AbandonedPartPublication<'_, '_>,
        node: NodeId,
    ) -> Result<CompletePublicationRequest<'a>, StoreError> {
        permit.require_member(node)?;
        let mut request = Self::abandoned_part_cleanup_request(permit)?;
        request.node_id = node;
        Ok(request)
    }

    pub(in crate::node_runtime) fn id(&self) -> MetadataCommandId {
        self.command.id()
    }
    pub(in crate::node_runtime) fn node_id(&self) -> NodeId {
        self.node_id
    }
    pub(in crate::node_runtime) fn topology(&self) -> &PgTopology {
        self.command.topology()
    }
    pub(in crate::node_runtime) fn command_sha256(&self) -> [u8; 32] {
        self.command.command_sha256()
    }
    pub(in crate::node_runtime) fn checksum_crc64(&self) -> u64 {
        self.command.checksum_crc64()
    }

    pub(in crate::node_runtime) fn bound_deadline(
        &self,
        deadline: std::time::Instant,
    ) -> std::time::Instant {
        deadline.min(self.work_deadline.capture(deadline).local)
    }

    /// Requires independently admitted primary authority. Success confirms only
    /// the marker, not replica application, terminal convergence or cleanup.
    pub(in crate::node_runtime) fn start(
        &self,
        pg: &PgStore,
        authority: &CompletePublicationAuthority<'_>,
    ) -> Result<(), CompletePublicationError> {
        self.start_inner(
            pg,
            PublicationAuthority::Remote(authority),
            #[cfg(test)]
            |_| Ok(()),
        )
    }

    pub(in crate::node_runtime) fn start_embedded(
        &self,
        pg: &PgStore,
        authority: &EmbeddedPublicationAuthority<'_>,
    ) -> Result<(), CompletePublicationError> {
        self.start_inner(
            pg,
            PublicationAuthority::Embedded(authority),
            #[cfg(test)]
            |_| Ok(()),
        )
    }

    pub(in crate::node_runtime::pg_store::command_log) fn start_effects(
        &self,
        pg: &PgStore,
        authority: PublicationAuthority<'_>,
        prefix: &mut PrefixTransition<'_>,
        #[cfg(test)] mut hook: impl FnMut(&'static str) -> Result<(), StoreError>,
    ) -> Result<(), CompletePublicationError> {
        let result = (|| -> Result<_, Failure> {
            pg.require_usable_connection()?;
            authority.require_effect(pg, self)?;
            if !pg.conn.is_autocommit() {
                return Err(StoreError::RouteCapabilitySubjectMismatch {
                    operation: "complete publication inside caller transaction",
                }
                .into());
            }
            self.command
                .application(self.topology())
                .map_err(Failure::from)
        })();
        let application = result.map_err(CompletePublicationError)?;
        let transaction = MetadataTransaction::begin(pg, "complete publication begin")?;
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
            #[cfg(test)]
            hook("before-trial")?;
            authority.require_effect(pg, self)?;
            prefix.before_effect()?;
            match pg.metadata_command_identity_acceptance(node, &identity)? {
                MetadataCommandAcceptance::Apply => {
                    let trial = MetadataTransaction::begin(pg, "complete publication savepoint")?;
                    let result = (|| -> Result<(), Failure> {
                        authority.require_effect(pg, self)?;
                        application.apply_in_open_transaction(pg, node)?;
                        prefix.after_effect()?;
                        #[cfg(test)]
                        hook("after-trial")?;
                        Ok(())
                    })();
                    match result {
                        Ok(()) => trial.rollback()?,
                        Err(error) => return Err(error.rollback(trial)),
                    }
                }
                MetadataCommandAcceptance::AlreadyApplied => {
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
                }
            }
            #[cfg(test)]
            hook("before-marker")?;
            prefix.before_effect()?;
            authority.require_effect(pg, self)?;
            if !slot.publication_started {
                let changed = pg.execute_cached(
                    "UPDATE metadata_command_pending_slot SET publication_started = 1 WHERE singleton = 0 AND cluster_epoch = ?1 AND pg_id = ?2 AND log_index = ?3 AND command_checksum = ?4 AND command_bytes = ?5 AND scope_bucket = ?6 AND publication_started = 0",
                    params![self.id().cluster_epoch().get() as i64, self.id().pg_id().get(), self.id().log_index().get() as i64, command.checksum_crc64() as i64, command.bytes(), command.scope_bucket()],
                    "mark complete pending publication started",
                )?;
                if changed != 1 {
                    return Err(identity.conflict(node, pg.pg_id).into());
                }
            }
            prefix.after_effect()?;
            #[cfg(test)]
            hook("before-commit")?;
            authority.require_effect(pg, self)?;
            Ok(())
        })();
        finish_transaction(transaction, result, "complete publication commit")?;
        #[cfg(test)]
        hook("after-commit")?;
        Ok(())
    }

    #[cfg(test)]
    pub(in crate::node_runtime) fn test_start_with_hook(
        &self,
        pg: &PgStore,
        authority: &CompletePublicationAuthority<'_>,
        hook: impl FnMut(&'static str) -> Result<(), StoreError>,
    ) -> Result<(), CompletePublicationError> {
        self.start_inner(pg, PublicationAuthority::Remote(authority), hook)
    }

    #[cfg(test)]
    pub(in crate::node_runtime) fn test_start_embedded_with_hook(
        &self,
        pg: &PgStore,
        authority: &EmbeddedPublicationAuthority<'_>,
        hook: impl FnMut(&'static str) -> Result<(), StoreError>,
    ) -> Result<(), CompletePublicationError> {
        self.start_inner(pg, PublicationAuthority::Embedded(authority), hook)
    }
}
