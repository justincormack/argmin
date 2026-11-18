// Copyright The Argmin Authors.
// SPDX-License-Identifier: Apache-2.0

//! Preservation, never validation: only the fixed entry points below can
//! create/finish this observer. Effect implementations borrow it and cannot
//! return it or install an independently supplied cache entry. No production
//! callback or caller-selected SQL runs within these boundaries.

use super::*;
use crate::node_runtime::engine::EmbeddedPublicationAuthority;
use crate::node_runtime::pg_store::payload_staging::EncodedContextualCommand;
use crate::node_runtime::server::{CompletePublicationAuthority, CompleteRetirementAuthority};

#[cfg(test)]
thread_local! {
    static LOSE_COMPLETE_INSTALL_CONFIRMATION: std::cell::Cell<bool> = const { std::cell::Cell::new(false) };
}

pub(in crate::node_runtime::pg_store::command_log) enum PublicationAuthority<'a> {
    Remote(&'a CompletePublicationAuthority<'a>),
    Embedded(&'a EmbeddedPublicationAuthority<'a>),
}

impl PublicationAuthority<'_> {
    pub(in crate::node_runtime::pg_store::command_log) fn require_effect(
        &self,
        pg: &PgStore,
        request: &CompletePublicationRequest<'_>,
    ) -> Result<(), StoreError> {
        match self {
            Self::Remote(authority) => authority.require_effect(pg, request),
            Self::Embedded(authority) => authority.require_effect(pg, request),
        }
    }
}

pub(in crate::node_runtime::pg_store::command_log) enum RetirementEffectAuthority<'a> {
    #[cfg(test)]
    Local(
        &'a CompletePublicationAuthority<'a>,
        &'a TerminalSlotRetirementAuthority<'a>,
    ),
    Embedded(
        &'a EmbeddedPublicationAuthority<'a>,
        &'a TerminalSlotRetirementAuthority<'a>,
    ),
    Remote(&'a CompleteRetirementAuthority<'a>),
}

impl RetirementEffectAuthority<'_> {
    pub(in crate::node_runtime::pg_store::command_log) fn require_effect(
        &self,
        pg: &PgStore,
        request: &CompletePublicationRequest<'_>,
    ) -> Result<(), StoreError> {
        match self {
            #[cfg(test)]
            Self::Local(authority, terminal) => {
                terminal.require_request(request)?;
                authority.require_effect(pg, request)
            }
            Self::Embedded(authority, terminal) => {
                terminal.require_request(request)?;
                authority.require_effect(pg, request)
            }
            Self::Remote(authority) => authority.require_effect(pg, request),
        }
    }
}

pub(in crate::node_runtime::pg_store::command_log) struct PrefixTransition<'pg> {
    pg: &'pg PgStore,
    entry: Option<ValidatedPrefix>,
    checked: bool,
}

impl<'pg> PrefixTransition<'pg> {
    fn new(pg: &'pg PgStore, id: MetadataCommandId, topology: &PgTopology) -> Self {
        let entry = pg
            .complete_prefix_validation
            .entry
            .borrow_mut()
            .take()
            .filter(|entry| {
                entry.pg_id == pg.pg_id
                    && entry.pg_id == id.pg_id().get()
                    && entry.state.cluster_epoch == id.cluster_epoch()
                    && entry.topology == *topology
            });
        Self {
            pg,
            entry,
            checked: false,
        }
    }

    fn stamp(&mut self) -> Result<Option<Stamp>, StoreError> {
        if self.entry.is_none() {
            return Ok(None);
        }
        match Stamp::read(self.pg) {
            Ok(stamp) => Ok(Some(stamp)),
            // Cache availability cannot prevent already-issued work converging.
            Err(StoreError::PgAllocationLifetimeUnavailable { .. }) => {
                self.entry = None;
                Ok(None)
            }
            Err(error) => Err(error),
        }
    }

    /// Called inside the existing write transaction, after admission/inspection
    /// and immediately before each closed block of effects. Reads pin the view
    /// before comparing the external-write stamp.
    pub(in crate::node_runtime::pg_store::command_log) fn before_effect(
        &mut self,
    ) -> Result<(), StoreError> {
        self.checked = false;
        if self.entry.is_none() {
            return Ok(());
        }
        let state = self.pg.metadata_command_replica_state()?;
        let stamp = self.stamp()?;
        if self.pg.conn.is_autocommit()
            || self
                .entry
                .as_ref()
                .is_none_or(|entry| entry.state != state || Some(entry.stamp) != stamp)
        {
            self.entry = None;
        } else {
            self.checked = true;
        }
        Ok(())
    }

    /// Freeze the stamp BEFORE commit, reply-loss hooks or another effect block.
    /// These transitions do not advance the replica prefix; only complete apply
    /// may do that. No cold entry can be manufactured here.
    pub(in crate::node_runtime::pg_store::command_log) fn after_effect(
        &mut self,
    ) -> Result<(), StoreError> {
        if !self.checked {
            self.entry = None;
            return Ok(());
        }
        self.checked = false;
        let state = self.pg.metadata_command_replica_state()?;
        let Some(stamp) = self.stamp()? else {
            return Ok(());
        };
        let entry = self
            .entry
            .as_mut()
            .expect("stamp requires existing validation");
        if self.pg.conn.is_autocommit()
            || entry.state != state
            || entry.stamp.lifetime != stamp.lifetime
            || entry.stamp.external_changes != stamp.external_changes
            || entry.stamp.schema_version != stamp.schema_version
        {
            self.entry = None;
        } else {
            entry.stamp = stamp;
        }
        Ok(())
    }

    fn finish(self) -> Result<(), StoreError> {
        if let Some(entry) = self.entry {
            // No return/Drop path may preserve after failed or uncertain finish.
            // The caller invokes this only after the entire fixed operation
            // succeeds; retain_committed compares against the pre-commit stamp.
            match self
                .pg
                .complete_prefix_validation
                .retain_committed(self.pg, entry)
            {
                Ok(()) | Err(StoreError::PgAllocationLifetimeUnavailable { .. }) => {}
                Err(error) => return Err(error),
            }
        }
        Ok(())
    }
}

impl PgStore {
    #[cfg(test)]
    pub(in crate::node_runtime) fn test_with_lost_complete_install_confirmation<T>(
        action: impl FnOnce() -> T,
    ) -> T {
        struct Reset(bool);
        impl Drop for Reset {
            fn drop(&mut self) {
                LOSE_COMPLETE_INSTALL_CONFIRMATION.set(self.0);
            }
        }
        let _reset = Reset(LOSE_COMPLETE_INSTALL_CONFIRMATION.replace(true));
        action()
    }

    pub(in crate::node_runtime) fn allocate_remote_confirmed_stream_window(
        &self,
        authority: &crate::node_runtime::server::RemoteStreamAllocationAuthority<'_>,
    ) -> Result<
        crate::StreamUploadSegmentRecord,
        crate::node_runtime::pg_store::StagingWindowConfirmationError,
    > {
        use crate::node_runtime::pg_store::metadata_transaction::MetadataTransaction;
        use crate::node_runtime::pg_store::StagingWindowConfirmationError as Error;
        authority.require_effect(self)?;
        let transaction =
            MetadataTransaction::begin_outer(self, "begin remote confirmed stream allocation")?;
        let query = authority.query();
        let mut prefix = None;
        let result = (|| {
            authority.require_effect(self)?;
            query.require_prefix(self)?;
            let transition =
                prefix.insert(PrefixTransition::new(self, query.id(), query.topology()));
            transition.before_effect()?;
            let segment = query.prepare(self, authority)?;
            transition.after_effect()?;
            authority.require_effect(self)?;
            Ok::<_, Error>(segment)
        })();
        let segment = match result {
            Ok(segment) => segment,
            Err(error) => {
                return match transaction.rollback() {
                    Ok(()) => Err(error),
                    Err(rollback) => Err(Error::Window(format!(
                        "remote allocation failed: {error}; rollback failed: {rollback}"
                    ))),
                }
            }
        };
        transaction.commit("commit remote confirmed stream allocation")?;
        prefix
            .expect("successful remote allocation has transition")
            .finish()?;
        Ok(segment)
    }

    pub(in crate::node_runtime) fn allocate_confirmed_stream_window(
        &self,
        permit: &crate::node_runtime::pg_store::StreamWindowAllocationPermit<'_, '_>,
        authority: &EmbeddedPublicationAuthority<'_>,
        request: &CompletePublicationRequest<'_>,
        placement: crate::node_runtime::payload_placement::PayloadPlacementInput<'_>,
        #[cfg(test)] mut hook: impl FnMut(&'static str) -> Result<(), StoreError>,
    ) -> Result<
        crate::StreamUploadSegmentRecord,
        crate::node_runtime::pg_store::StagingWindowConfirmationError,
    > {
        use crate::node_runtime::pg_store::metadata_transaction::MetadataTransaction;
        use crate::node_runtime::pg_store::StagingWindowConfirmationError as Error;
        permit.require_effect(self, authority, request)?;
        let transaction =
            MetadataTransaction::begin_outer(self, "begin confirmed stream allocation")?;
        let mut prefix = None;
        let result = (|| {
            permit.require_effect(self, authority, request)?;
            permit.require_prefix(self, request)?;
            let transition = prefix.insert(PrefixTransition::new(
                self,
                request.id(),
                request.topology(),
            ));
            transition.before_effect()?;
            #[cfg(test)]
            hook("before-allocation")?;
            let segment =
                permit.prepare_in_open_transaction(self, authority, request, placement)?;
            transition.after_effect()?;
            #[cfg(test)]
            hook("before-commit")?;
            permit.require_effect(self, authority, request)?;
            Ok::<_, Error>(segment)
        })();
        let segment = match result {
            Ok(segment) => segment,
            Err(error) => {
                return match transaction.rollback() {
                    Ok(()) => Err(error),
                    Err(rollback) => Err(Error::Window(format!(
                        "stream allocation failed: {error}; rollback failed: {rollback}"
                    ))),
                };
            }
        };
        transaction.commit("commit confirmed stream allocation")?;
        prefix
            .expect("successful allocation has a transition")
            .finish()?;
        #[cfg(test)]
        hook("after-commit")?;
        Ok(segment)
    }

    pub(in crate::node_runtime::pg_store) fn try_insert_complete_pending_metadata_command_slot(
        &self,
        node_id: u32,
        command: &EncodedContextualCommand<'_>,
        topology: &PgTopology,
        effect_fence: AdmittedRouteEffectFence,
        operation_deadline: Option<Instant>,
    ) -> Result<(), PendingMetadataCommandSlotInsertError> {
        self.try_insert_complete_pending_with_direct_proof(
            node_id,
            command,
            topology,
            effect_fence,
            operation_deadline,
            None,
        )
    }

    pub(in crate::node_runtime::pg_store) fn try_insert_complete_pending_with_direct_proof(
        &self,
        node_id: u32,
        command: &EncodedContextualCommand<'_>,
        topology: &PgTopology,
        effect_fence: AdmittedRouteEffectFence,
        operation_deadline: Option<Instant>,
        direct: Option<super::payload_staging::DirectInstallationCheck<'_, '_>>,
    ) -> Result<(), PendingMetadataCommandSlotInsertError> {
        let mut prefix = PrefixTransition::new(self, command.id(), topology);
        self.try_insert_projected_pending_metadata_command_slot(
            node_id,
            PendingInstallation::Complete {
                command,
                topology,
                direct,
            },
            Some(command.scope_bucket()),
            effect_fence,
            operation_deadline,
            Some(&mut prefix),
        )?;
        prefix
            .finish()
            .map_err(PendingMetadataCommandSlotInsertError::may_have_applied)?;
        #[cfg(test)]
        if LOSE_COMPLETE_INSTALL_CONFIRMATION.replace(false) {
            return Err(PendingMetadataCommandSlotInsertError::may_have_applied(
                StoreError::OperationDeadlineExceeded {
                    context: "injected lost complete installation confirmation",
                },
            ));
        }
        Ok(())
    }
}

impl CompletePublicationRequest<'_> {
    pub(in crate::node_runtime::pg_store::command_log) fn start_inner(
        &self,
        pg: &PgStore,
        authority: PublicationAuthority<'_>,
        #[cfg(test)] hook: impl FnMut(&'static str) -> Result<(), StoreError>,
    ) -> Result<(), CompletePublicationError> {
        let mut prefix = PrefixTransition::new(pg, self.id(), self.topology());
        self.start_effects(
            pg,
            authority,
            &mut prefix,
            #[cfg(test)]
            hook,
        )?;
        prefix.finish()?;
        Ok(())
    }

    pub(in crate::node_runtime::pg_store::command_log) fn retire_inner(
        &self,
        pg: &PgStore,
        authority: RetirementEffectAuthority<'_>,
        #[cfg(test)] hook: impl FnMut(&'static str) -> Result<(), StoreError>,
    ) -> Result<(), CompletePublicationError> {
        let mut prefix = PrefixTransition::new(pg, self.id(), self.topology());
        self.retire_effects(
            pg,
            authority,
            &mut prefix,
            #[cfg(test)]
            hook,
        )?;
        prefix.finish()?;
        Ok(())
    }
}
