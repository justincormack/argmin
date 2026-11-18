// Copyright The Argmin Authors.
// SPDX-License-Identifier: Apache-2.0

//! Current embedded-primary replacement, without detaching the abandoned source
//! or its cleanup. Any entered storage attempt keeps uncertainty until confirmed.

use super::*;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(in crate::node_runtime) enum AbandonedPartInstallationObservation {
    NoAttemptObserved,
    Unconfirmed,
    Installed,
}

/// Only the owning pair can lend this exclusive attempt. No constructor, raw
/// pair, callback, cloned history or independently installable context escapes.
pub(in crate::node_runtime) struct AbandonedPartInstallation<'a, 'route> {
    source: &'a CompleteAbandonmentHandoff<'route>,
    candidate: &'a Candidate,
    observation: &'a mut AbandonedPartInstallationObservation,
}

impl PreparedAbandonedPart<'_> {
    pub(in crate::node_runtime) fn cleanup_installation(
        &self,
    ) -> AbandonedPartInstallationObservation {
        self.installation
    }

    pub(in crate::node_runtime) fn install_cleanup(
        &mut self,
        section: &EmbeddedPublicationSection<'_>,
    ) -> Result<(), CompletePublicationError> {
        self.install_cleanup_inner(
            section,
            #[cfg(test)]
            |_| Ok(()),
        )
    }

    #[cfg(test)]
    pub(in crate::node_runtime::pg_store) fn test_install_cleanup(
        &mut self,
        section: &EmbeddedPublicationSection<'_>,
        hook: impl FnMut(&'static str) -> Result<(), StoreError>,
    ) -> Result<(), CompletePublicationError> {
        self.install_cleanup_inner(section, hook)
    }

    fn install_cleanup_inner(
        &mut self,
        section: &EmbeddedPublicationSection<'_>,
        #[cfg(test)] hook: impl FnMut(&'static str) -> Result<(), StoreError>,
    ) -> Result<(), CompletePublicationError> {
        let sender = &self.owner.owner.owner;
        let request = sender
            .sender
            .source
            .request(sender.route.primary())
            .map_err(|_| StoreError::RouteCapabilitySubjectMismatch {
                operation: "abandoned part installation original source",
            })?;
        let attempt = AbandonedPartInstallation {
            source: &self.owner,
            candidate: &self.candidate,
            observation: &mut self.installation,
        };
        section.install_abandoned_part_cleanup(
            &request,
            attempt,
            sender.work_deadline(),
            #[cfg(test)]
            hook,
        )
    }
}

impl AbandonedPartInstallation<'_, '_> {
    pub(in crate::node_runtime) fn require_embedded_section(
        &self,
        section: &EmbeddedPublicationSection<'_>,
    ) -> Result<(), StoreError> {
        AbandonedPartPreparation { owner: self.source }.require_embedded_section(section)
    }

    pub(in crate::node_runtime) fn require_request(
        &self,
        request: &CompletePublicationRequest<'_>,
    ) -> Result<(), StoreError> {
        self.source.require_valid()?;
        let sender = &self.source.owner.owner;
        let original = sender
            .sender
            .source
            .request(sender.route.primary())
            .map_err(|_| StoreError::RouteCapabilitySubjectMismatch {
                operation: "abandoned part installation original encoding",
            })?;
        if request.id() != original.id()
            || request.command_sha256() != original.command_sha256()
            || request.topology() != original.topology()
            || request.node_id() != original.node_id()
        {
            return Err(StoreError::RouteCapabilitySubjectMismatch {
                operation: "abandoned part installation exact source/primary",
            });
        }
        Ok(())
    }

    pub(in crate::node_runtime) fn execute_in_store(
        self,
        pg: &PgStore,
        authority: &EmbeddedPublicationAuthority<'_>,
        request: &CompletePublicationRequest<'_>,
        #[cfg(test)] hook: impl FnMut(&'static str) -> Result<(), StoreError>,
    ) -> Result<(), CompletePublicationError> {
        authority.require_abandoned_part_installation(&self)?;
        authority.require_effect(pg, request)?;
        self.require_request(request)?;
        match *self.observation {
            // Installation is an observed fact, not a claim that the slot has
            // remained unchanged. Validate the caller's authority above, but
            // never redispatch or erase a confirmed result. Later publication
            // and terminal transitions must validate their own prerequisites.
            AbandonedPartInstallationObservation::Installed => return Ok(()),
            AbandonedPartInstallationObservation::NoAttemptObserved
            | AbandonedPartInstallationObservation::Unconfirmed => {
                // Before any SQL entry, including unwind. A later failed
                // preflight cannot erase earlier uncertainty either.
                *self.observation = AbandonedPartInstallationObservation::Unconfirmed;
            }
        }
        request.install_abandoned_part(
            pg,
            authority,
            &self,
            #[cfg(test)]
            hook,
        )?;
        *self.observation = AbandonedPartInstallationObservation::Installed;
        Ok(())
    }

    pub(in crate::node_runtime::pg_store) fn cleanup(
        &self,
    ) -> Result<EncodedContextualCommand<'_>, AbandonedPartPreparationError> {
        self.candidate.cleanup.encode_command().map_err(|_| {
            AbandonedPartPreparationError(ContextError::Encoding(
                "encode prepared abandoned part cleanup",
            ))
        })
    }

    pub(in crate::node_runtime::pg_store) fn prefix(&self) -> MetadataCommandReplicaState {
        self.candidate.prefix
    }

    /// Rederive the closure, including its exact original finalizer proof, under
    /// write serialization. The earlier read snapshot cannot authorize new rows.
    pub(in crate::node_runtime::pg_store) fn require_fresh_closure(
        &self,
        pg: &PgStore,
        node: u32,
        source: &EncodedContextualCommand<'_>,
    ) -> Result<(), AbandonedPartPreparationError> {
        let permit = AbandonedPartPreparation { owner: self.source };
        let current = source
            .capture_admitted_part_cleanup(&permit, pg, node, self.candidate.cleanup.id())
            .map_err(AbandonedPartPreparationError)?;
        let current = current.encode_command().map_err(|_| {
            AbandonedPartPreparationError(ContextError::Encoding(
                "encode recaptured abandoned part cleanup",
            ))
        })?;
        if current.bytes() != self.cleanup()?.bytes() {
            return Err(AbandonedPartPreparationError(ContextError::Encoding(
                "abandoned part cleanup closure changed since preparation",
            )));
        }
        Ok(())
    }
}
