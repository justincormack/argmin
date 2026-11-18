// Copyright The Argmin Authors.
// SPDX-License-Identifier: Apache-2.0

//! Read-only, current-primary UploadPart cleanup preparation. Only the owning
//! all-member handoff lends this permit. No candidate or renewed history escapes.

use super::*;
use crate::cluster::EmbeddedPublicationSection;
use crate::metadata_command::{MetadataCommandLogIndex, MetadataCommandReplicaState};
use crate::node_runtime::engine::EmbeddedPublicationAuthority;
use crate::node_runtime::pg_store::payload_staging::ContextError;
use crate::node_runtime::pg_store::payload_staging::EncodedContextualCommand;
use crate::node_runtime::pg_store::{MetadataCommandStartupDisposition, PgStore};

#[path = "complete_abandonment_installation.rs"]
mod installation;
pub(in crate::node_runtime) use installation::{
    AbandonedPartInstallation, AbandonedPartInstallationObservation,
};
#[path = "complete_abandoned_part_publication.rs"]
mod publication;
pub(in crate::node_runtime) use publication::AbandonedPartPublication;

#[path = "complete_abandoned_part_terminal.rs"]
mod terminal;
pub(in crate::node_runtime) use terminal::AbandonedPartFinalizerRelease;
pub(in crate::node_runtime) use terminal::{
    AbandonedPartTerminalFailure, AbandonedPartTerminalHandoff,
};

pub(in crate::node_runtime) struct AbandonedPartPreparation<'a, 'route> {
    owner: &'a CompleteAbandonmentHandoff<'route>,
}

#[derive(Debug)]
pub(in crate::node_runtime) struct AbandonedPartPreparationError(ContextError);

impl std::fmt::Display for AbandonedPartPreparationError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.0.fmt(f)
    }
}
impl std::error::Error for AbandonedPartPreparationError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        Some(&self.0)
    }
}

impl From<StoreError> for AbandonedPartPreparationError {
    fn from(error: StoreError) -> Self {
        Self(error.into())
    }
}

struct Candidate {
    cleanup: ContextualCommand,
    prefix: MetadataCommandReplicaState,
}

/// The receiver cannot detach a candidate from the permit/source it borrowed.
pub(in crate::node_runtime) struct PreparedAbandonedPartBorrow<'a> {
    candidate: Candidate,
    _source: std::marker::PhantomData<&'a ()>,
}

#[must_use = "retain original abandonment custody together with the unsent cleanup"]
pub(in crate::node_runtime) struct PreparedAbandonedPart<'route> {
    owner: CompleteAbandonmentHandoff<'route>,
    candidate: Candidate,
    installation: AbandonedPartInstallationObservation,
    publication: PublicationObservation,
    applications: std::collections::BTreeMap<NodeId, ApplicationObservation>,
}

#[must_use = "preparation failure retains the complete original abandonment holder"]
pub(in crate::node_runtime) struct AbandonedPartPreparationFailure<'route> {
    owner: Box<CompleteAbandonmentHandoff<'route>>,
    error: AbandonedPartPreparationError,
}

impl<'route> CompleteAbandonmentHandoff<'route> {
    pub(in crate::node_runtime) fn prepare_part_cleanup(
        self,
        section: &EmbeddedPublicationSection<'_>,
    ) -> Result<PreparedAbandonedPart<'route>, AbandonedPartPreparationFailure<'route>> {
        self.prepare_part_cleanup_inner(
            section,
            #[cfg(test)]
            |_| Ok(()),
        )
    }

    #[cfg(test)]
    pub(in crate::node_runtime::pg_store) fn test_prepare_part_cleanup(
        self,
        section: &EmbeddedPublicationSection<'_>,
        hook: impl FnMut(&'static str) -> Result<(), StoreError>,
    ) -> Result<PreparedAbandonedPart<'route>, AbandonedPartPreparationFailure<'route>> {
        self.prepare_part_cleanup_inner(section, hook)
    }

    fn prepare_part_cleanup_inner(
        self,
        section: &EmbeddedPublicationSection<'_>,
        #[cfg(test)] hook: impl FnMut(&'static str) -> Result<(), StoreError>,
    ) -> Result<PreparedAbandonedPart<'route>, AbandonedPartPreparationFailure<'route>> {
        let prepare = || {
            let permit = AbandonedPartPreparation { owner: &self };
            permit.require_embedded_section(section)?;
            let sender = &self.owner.owner;
            let request = sender
                .sender
                .source
                .request(sender.route.primary())
                .map_err(|_| {
                    AbandonedPartPreparationError(ContextError::Encoding(
                        "retained finalizer publication request",
                    ))
                })?;
            let prepared = section.prepare_abandoned_part_cleanup(
                &request,
                &permit,
                sender.work_deadline(),
                #[cfg(test)]
                hook,
            )?;
            Ok(prepared.candidate)
        };
        match prepare() {
            Ok(candidate) => Ok(PreparedAbandonedPart {
                owner: self,
                candidate,
                installation: AbandonedPartInstallationObservation::NoAttemptObserved,
                publication: PublicationObservation::NoAttemptObserved,
                applications: std::collections::BTreeMap::new(),
            }),
            Err(error) => Err(AbandonedPartPreparationFailure {
                owner: Box::new(self),
                error,
            }),
        }
    }
}

impl AbandonedPartPreparation<'_, '_> {
    pub(in crate::node_runtime) fn require_embedded_section(
        &self,
        section: &EmbeddedPublicationSection<'_>,
    ) -> Result<(), StoreError> {
        self.owner.require_valid()?;
        let route = &self.owner.owner.owner.route;
        // Historical source admission is NOT authority to mint a current-epoch
        // successor. That separate admitted transfer remains an explicit gate.
        if route.is_historical() {
            return Err(StoreError::RouteCapabilitySubjectMismatch {
                operation: "abandoned part preparation requires current primary admission",
            });
        }
        route.require_embedded_section(section)
    }

    pub(in crate::node_runtime::pg_store) fn require_context(
        &self,
        command: &EncodedContextualCommand<'_>,
    ) -> Result<(), StoreError> {
        self.owner.require_valid()?;
        let sender = &self.owner.owner.owner;
        let request = sender
            .sender
            .source
            .request(sender.route.primary())
            .map_err(|_| StoreError::RouteCapabilitySubjectMismatch {
                operation: "abandoned part preparation source encoding",
            })?;
        if request.id() != command.id()
            || request.command_sha256() != command.command_sha256()
            || request.topology() != command.topology()
        {
            return Err(StoreError::RouteCapabilitySubjectMismatch {
                operation: "abandoned part preparation complete source",
            });
        }
        Ok(())
    }

    pub(in crate::node_runtime) fn prepare_in_store<'a>(
        &'a self,
        pg: &PgStore,
        authority: &EmbeddedPublicationAuthority<'_>,
        request: &CompletePublicationRequest<'_>,
        #[cfg(test)] hook: impl FnMut(&'static str) -> Result<(), StoreError>,
    ) -> Result<PreparedAbandonedPartBorrow<'a>, AbandonedPartPreparationError> {
        self.capture_in_store(
            pg,
            authority,
            request,
            #[cfg(test)]
            hook,
        )
        .map_err(AbandonedPartPreparationError)
    }

    fn capture_in_store<'a>(
        &'a self,
        pg: &PgStore,
        authority: &EmbeddedPublicationAuthority<'_>,
        request: &CompletePublicationRequest<'_>,
        #[cfg(test)] mut hook: impl FnMut(&'static str) -> Result<(), StoreError>,
    ) -> Result<PreparedAbandonedPartBorrow<'a>, ContextError> {
        authority.require_abandoned_part_preparation(self)?;
        authority.require_effect(pg, request)?;
        let snapshot = pg.command_build_snapshot()?;
        let retained = pg
            .load_complete_pending_in_open_transaction(
                request.node_id().as_u32(),
                request.topology(),
            )?
            .ok_or(ContextError::Encoding(
                "abandoned part source slot is absent",
            ))?;
        let encoded = retained
            .context
            .encode_command()
            .map_err(|_| ContextError::Encoding("abandoned part source encoding"))?;
        self.require_context(&encoded)?;
        if request.id() != encoded.id()
            || request.command_sha256() != encoded.command_sha256()
            || request.topology() != encoded.topology()
            || retained.observation.publication_started
            || !matches!(
                retained.observation.terminal,
                MetadataCommandStartupDisposition::Abandoned { .. }
            )
        {
            return Err(ContextError::Encoding(
                "abandoned part requires exact unstarted tombstone/slot",
            ));
        }
        let prefix = pg.metadata_command_replica_state()?;
        if prefix.cluster_epoch != request.id().cluster_epoch() {
            return Err(ContextError::ReissuePosition);
        }
        let next = prefix
            .applied_log_index
            .checked_add(1)
            .and_then(MetadataCommandLogIndex::new)
            .ok_or(ContextError::ReissuePosition)?;
        let id = MetadataCommandId::new(prefix.cluster_epoch, request.id().pg_id(), next);
        let cleanup =
            encoded.capture_admitted_part_cleanup(self, pg, request.node_id().as_u32(), id)?;
        #[cfg(test)]
        hook("after-capture")?;
        snapshot.finish()?;
        authority.require_effect(pg, request)?;
        self.owner.require_valid()?;
        Ok(PreparedAbandonedPartBorrow {
            candidate: Candidate { cleanup, prefix },
            _source: std::marker::PhantomData,
        })
    }
}

impl<'route> AbandonedPartPreparationFailure<'route> {
    pub(in crate::node_runtime) fn error(&self) -> &AbandonedPartPreparationError {
        &self.error
    }
    pub(in crate::node_runtime) fn into_handoff(self) -> CompleteAbandonmentHandoff<'route> {
        *self.owner
    }
}

impl PreparedAbandonedPart<'_> {
    pub(in crate::node_runtime) fn source_id(&self) -> MetadataCommandId {
        self.owner.id()
    }
    pub(in crate::node_runtime) fn cleanup_id(&self) -> MetadataCommandId {
        self.candidate.cleanup.id()
    }
    pub(in crate::node_runtime) fn prefix(&self) -> MetadataCommandReplicaState {
        self.candidate.prefix
    }
    pub(in crate::node_runtime) fn require_valid(&self) -> Result<(), StoreError> {
        self.owner.require_valid()
    }

    #[cfg(test)]
    pub(in crate::node_runtime::pg_store) fn assert_contexts_for_test(
        &self,
        source: &ContextualCommand,
        cleanup: &ContextualCommand,
        topology: &crate::PgTopology,
    ) {
        self.owner.assert_context_for_test(source, topology);
        assert_eq!(
            self.candidate.cleanup.encode_command().unwrap().bytes(),
            cleanup.encode_command().unwrap().bytes()
        );
    }

    #[cfg(test)]
    pub(in crate::node_runtime::pg_store) fn installation_observation(
        &self,
    ) -> Option<ContextSubmissionObservation> {
        self.owner.installation_observation()
    }
}
