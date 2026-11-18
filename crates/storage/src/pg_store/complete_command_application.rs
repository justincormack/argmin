// Copyright The Argmin Authors.
// SPDX-License-Identifier: Apache-2.0

//! Coordinated complete-carrier application. The encoded input borrows its
//! retained context, so acceptance, mutation and recording cannot be supplied
//! different commands. This is not yet selected by production RPC dispatch.

use super::*;
use crate::node_runtime::engine::EmbeddedApplicationAuthority;
use crate::node_runtime::pg_store::command_log::complete_pending_inspection::CompletePublicationTransport;
use crate::node_runtime::pg_store::metadata_transaction::MetadataTransaction;
use crate::node_runtime::server::CompleteApplicationAuthority;
use crate::PgTopology;
use payload_staging::{ApplicationError, EncodedContextualCommand};

#[path = "complete_application_transport.rs"]
mod transport;
pub(in crate::node_runtime) use transport::CompleteApplicationQuery;

enum ApplicationAdmission<'a> {
    Admitted(&'a CompleteApplicationAuthority<'a>),
    Embedded(&'a EmbeddedApplicationAuthority<'a>),
    #[cfg(test)]
    Fixture,
}

impl ApplicationAdmission<'_> {
    fn require_effect(&self, pg: &PgStore) -> Result<(), StoreError> {
        match self {
            Self::Admitted(authority) => authority.require_effect(pg, authority.request()),
            Self::Embedded(authority) => authority.require_effect(pg, authority.request()),
            #[cfg(test)]
            Self::Fixture => Ok(()),
        }
    }
}

/// Receiver-side request view; the complete representation stays inside storage.
/// This is not sender custody, permission to apply, or a renewable deadline.
pub(in crate::node_runtime) struct CompleteApplicationRequest<'a> {
    command: &'a EncodedContextualCommand<'a>,
    node_id: NodeId,
    deadline: Instant,
}

impl<'a> CompleteApplicationRequest<'a> {
    /// Borrow the original retained command and already-frozen cutoff. Unlike
    /// the test constructor this takes no independently supplied command or time.
    pub(in crate::node_runtime::pg_store::command_log) fn from_publication_transport(
        transport: &'a CompletePublicationTransport<'a>,
    ) -> Self {
        Self {
            command: transport.application_command(),
            node_id: transport.node_id(),
            deadline: transport.deadline(),
        }
    }

    #[cfg(test)]
    pub(in crate::node_runtime::pg_store) fn new(
        node_id: NodeId,
        command: &'a EncodedContextualCommand<'a>,
        deadline: Instant,
    ) -> Self {
        Self {
            command,
            node_id,
            deadline,
        }
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
    pub(in crate::node_runtime) fn deadline(&self) -> Instant {
        self.deadline
    }
}

#[derive(Debug, thiserror::Error)]
enum Failure {
    #[error(transparent)]
    Store(#[from] StoreError),
    #[error(transparent)]
    Application(#[from] ApplicationError),
    #[error("complete command application failed: {cause}; rollback failed: {rollback}")]
    Rollback {
        cause: Box<Self>,
        rollback: StoreError,
    },
}

/// No error from apply grants absence, retirement or frontend cleanup authority.
/// In particular a failed commit may have applied; exact replay must establish
/// the durable outcome using the retained complete command.
#[derive(Debug, thiserror::Error)]
#[error(transparent)]
pub(in crate::node_runtime) struct CompleteApplicationError(Failure);

impl From<StoreError> for CompleteApplicationError {
    fn from(error: StoreError) -> Self {
        Self(Failure::Store(error))
    }
}

impl CompleteApplicationError {
    pub(in crate::node_runtime) fn into_store_error(self) -> Result<StoreError, Self> {
        match self.0 {
            Failure::Store(error) => Ok(error),
            other => Err(Self(other)),
        }
    }
}

// An existing validation may cross ONLY this closed apply/record transaction.
// Consume it after admission/acceptance and test hooks, immediately before the
// first write. Cold/changed entries remain cold; this is never implicit recovery.
fn take_apply_prefix(
    pg: &PgStore,
    command: &EncodedContextualCommand<'_>,
) -> Result<Option<ValidatedPrefix>, StoreError> {
    let Some(entry) = pg.complete_prefix_validation.entry.borrow_mut().take() else {
        return Ok(None);
    };
    let id = command.id();
    if pg.conn.is_autocommit()
        || entry.pg_id != pg.pg_id
        || entry.topology != *command.topology()
        || id.pg_id().get() != pg.pg_id
        || id.cluster_epoch() != entry.state.cluster_epoch
        || entry.state.applied_log_index.checked_add(1) != Some(id.log_index().get())
        || pg.metadata_command_replica_state()? != entry.state
    {
        return Ok(None);
    }
    let stamp = match Stamp::read(pg) {
        Ok(stamp) => stamp,
        // Retirement revokes allocation/confirmation authority, not permission
        // to converge an already-issued command. Discard the taken cache entry
        // and use the same application path as a cold store. Connection and SQL
        // errors remain fatal; this is not a general best-effort stamp read.
        Err(StoreError::PgAllocationLifetimeUnavailable { .. }) => return Ok(None),
        Err(error) => return Err(error),
    };
    if stamp != entry.stamp {
        return Ok(None);
    }
    Ok(Some(entry))
}

impl Failure {
    fn rollback(self, transaction: MetadataTransaction<'_>) -> CompleteApplicationError {
        CompleteApplicationError(match transaction.rollback() {
            Ok(()) => self,
            Err(rollback) => Failure::Rollback {
                cause: Box::new(self),
                rollback,
            },
        })
    }
}

impl PgStore {
    pub(in crate::node_runtime) fn apply_embedded_complete_metadata_command(
        &self,
        authority: &EmbeddedApplicationAuthority<'_>,
    ) -> Result<MetadataCommandReplicaState, CompleteApplicationError> {
        self.apply_complete_metadata_command_and_record_inner(
            authority.request().node_id().as_u32(),
            authority.request().command,
            authority.request().topology(),
            ApplicationAdmission::Embedded(authority),
            #[cfg(test)]
            || Ok(()),
            #[cfg(test)]
            |_| Ok(()),
        )
    }

    #[cfg(test)]
    pub(in crate::node_runtime) fn test_apply_embedded_complete_command_with_hook(
        &self,
        authority: &EmbeddedApplicationAuthority<'_>,
        hook: impl FnMut(&'static str) -> Result<(), StoreError>,
    ) -> Result<MetadataCommandReplicaState, CompleteApplicationError> {
        self.apply_complete_metadata_command_and_record_inner(
            authority.request().node_id().as_u32(),
            authority.request().command,
            authority.request().topology(),
            ApplicationAdmission::Embedded(authority),
            || Ok(()),
            hook,
        )
    }

    pub(in crate::node_runtime) fn apply_admitted_complete_metadata_command(
        &self,
        authority: &CompleteApplicationAuthority<'_>,
    ) -> Result<MetadataCommandReplicaState, CompleteApplicationError> {
        self.apply_complete_metadata_command_and_record_inner(
            authority.node_id().as_u32(),
            authority.request().command,
            authority.request().topology(),
            ApplicationAdmission::Admitted(authority),
            #[cfg(test)]
            || Ok(()),
            #[cfg(test)]
            |_| Ok(()),
        )
    }

    // Raw application is only fixture setup, never a production escape around
    // admitted replica authority. The same transaction body is exercised below.
    #[cfg(test)]
    pub(in crate::node_runtime::pg_store) fn apply_complete_metadata_command_and_record(
        &self,
        node_id: u32,
        command: &EncodedContextualCommand<'_>,
        topology: &PgTopology,
    ) -> Result<MetadataCommandReplicaState, CompleteApplicationError> {
        self.apply_complete_metadata_command_and_record_inner(
            node_id,
            command,
            topology,
            ApplicationAdmission::Fixture,
            #[cfg(test)]
            || Ok(()),
            |_| Ok(()),
        )
    }

    /// Inject a failure after fresh owner/log mutations but before COMMIT.
    /// Exact replay never invokes this hook, and production has no callback.
    #[cfg(test)]
    pub(in crate::node_runtime::pg_store) fn test_apply_complete_metadata_command_with_commit_guard(
        &self,
        node_id: u32,
        command: &EncodedContextualCommand<'_>,
        topology: &PgTopology,
        commit_guard: impl FnOnce() -> Result<(), StoreError>,
    ) -> Result<MetadataCommandReplicaState, CompleteApplicationError> {
        self.apply_complete_metadata_command_and_record_inner(
            node_id,
            command,
            topology,
            ApplicationAdmission::Fixture,
            commit_guard,
            |_| Ok(()),
        )
    }

    #[cfg(test)]
    pub(in crate::node_runtime::pg_store) fn test_apply_complete_command_with_hook(
        &self,
        command: &EncodedContextualCommand<'_>,
        hook: impl FnMut(&'static str) -> Result<(), StoreError>,
    ) -> Result<MetadataCommandReplicaState, CompleteApplicationError> {
        self.apply_complete_metadata_command_and_record_inner(
            0,
            command,
            command.topology(),
            ApplicationAdmission::Fixture,
            || Ok(()),
            hook,
        )
    }

    #[cfg(test)]
    pub(in crate::node_runtime) fn test_apply_admitted_complete_command_with_hook(
        &self,
        authority: &CompleteApplicationAuthority<'_>,
        hook: impl FnMut(&'static str) -> Result<(), StoreError>,
    ) -> Result<MetadataCommandReplicaState, CompleteApplicationError> {
        self.apply_complete_metadata_command_and_record_inner(
            authority.node_id().as_u32(),
            authority.request().command,
            authority.request().topology(),
            ApplicationAdmission::Admitted(authority),
            || Ok(()),
            hook,
        )
    }

    fn apply_complete_metadata_command_and_record_inner(
        &self,
        node_id: u32,
        command: &EncodedContextualCommand<'_>,
        topology: &PgTopology,
        admission: ApplicationAdmission<'_>,
        #[cfg(test)] commit_guard: impl FnOnce() -> Result<(), StoreError>,
        #[cfg(test)] mut hook: impl FnMut(&'static str) -> Result<(), StoreError>,
    ) -> Result<MetadataCommandReplicaState, CompleteApplicationError> {
        self.require_usable_connection()
            .map_err(|error| CompleteApplicationError(error.into()))?;
        admission
            .require_effect(self)
            .map_err(|error| CompleteApplicationError(error.into()))?;
        if !self.conn.is_autocommit() {
            return Err(CompleteApplicationError(
                StoreError::RouteCapabilitySubjectMismatch {
                    operation: "apply complete command inside caller transaction",
                }
                .into(),
            ));
        }
        let application = command
            .application(topology)
            .map_err(|error| CompleteApplicationError(error.into()))?;
        let transaction =
            MetadataTransaction::begin(self, "apply complete command and record (begin txn)")?;
        let identity = TerminalCommandIdentity::complete(command);
        let mut successor = None;
        let result = (|| -> Result<MetadataCommandRecordResult, Failure> {
            // BEGIN IMMEDIATE and acceptance reads may wait. Revalidate next
            // to the first write, not just before transaction admission.
            admission.require_effect(self)?;
            let record = match self.metadata_command_identity_acceptance(node_id, &identity)? {
                MetadataCommandAcceptance::Apply => {
                    #[cfg(test)]
                    hook("before-effect")?;
                    let validated = take_apply_prefix(self, command)?;
                    admission.require_effect(self)?;
                    application.apply_in_open_transaction(self, node_id)?;
                    let record =
                        self.record_metadata_command_identity_applied_inner(node_id, &identity)?;
                    if let Some(mut validated) = validated {
                        // The existing recorder validates the next link and
                        // materialized digest. Do not carry evidence through an
                        // epoch reset or any unexpectedly advanced tail.
                        if record.state.cluster_epoch == command.id().cluster_epoch()
                            && record.state.applied_log_index == command.id().log_index().get()
                        {
                            validated.state = record.state;
                            validated.stamp = Stamp::read(self)?;
                            successor = Some(validated);
                        }
                    }
                    #[cfg(test)]
                    commit_guard()?;
                    #[cfg(test)]
                    hook("after-effect")?;
                    record
                }
                MetadataCommandAcceptance::AlreadyApplied => {
                    // Owner/session transfers and the log were committed together.
                    // Re-running the fresh handler could require a now-removed
                    // session or consume obligations belonging to a successor.
                    let state = self.metadata_command_replica_state()?;
                    if state.cluster_epoch != command.id().cluster_epoch()
                        || state.applied_log_index < command.id().log_index().get()
                        || !matches!(
                            self.metadata_command_identity_startup_disposition(node_id, &identity)?,
                            MetadataCommandStartupDisposition::Applied { .. }
                        )
                    {
                        return Err(identity.conflict(node_id, self.pg_id).into());
                    }
                    MetadataCommandRecordResult {
                        state,
                        digest_revision: self.metadata_digest_revision()?,
                    }
                }
            };
            #[cfg(test)]
            hook("before-commit")?;
            admission.require_effect(self)?;
            Ok(record)
        })();
        match result {
            Ok(record) => {
                transaction.commit("apply complete command and record (commit txn)")?;
                self.mark_metadata_state_digest_clean_at_revision(record.digest_revision);
                #[cfg(test)]
                hook("after-commit").map_err(|error| CompleteApplicationError(error.into()))?;
                if let Some(successor) = successor {
                    // Compare the stamp captured before commit/hooks; never
                    // bless an intervening write by sampling a fresh identity.
                    self.complete_prefix_validation
                        .retain_committed(self, successor)?;
                }
                Ok(record.state)
            }
            Err(error) => Err(error.rollback(transaction)),
        }
    }
}
