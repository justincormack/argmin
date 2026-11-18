// Copyright The Argmin Authors.
// SPDX-License-Identifier: Apache-2.0

//! Publish the installed cleanup without detaching its abandoned source or
//! resetting either command's history/deadline. No terminal transfer gate.

use super::*;

#[path = "complete_abandoned_part_application.rs"]
mod application;

/// Only the owning, installed pair can lend this permit. Publication authority
/// alone cannot substitute another cleanup or discard the source certificate.
pub(in crate::node_runtime) struct AbandonedPartPublication<'a, 'route> {
    source: &'a CompleteAbandonmentHandoff<'route>,
    candidate: &'a Candidate,
    installation: &'a AbandonedPartInstallationObservation,
}

impl PreparedAbandonedPart<'_> {
    pub(super) fn inspect_cleanup_custody(
        &self,
        section: &EmbeddedPublicationSection<'_>,
    ) -> Result<
        crate::node_runtime::pg_store::AbandonedPartCustodyObservation,
        crate::node_runtime::pg_store::AbandonedPartCustodyError,
    > {
        let permit = AbandonedPartPublication {
            source: &self.owner,
            candidate: &self.candidate,
            installation: &self.installation,
        };
        section.inspect_abandoned_part_custody(&permit, self.owner.owner.owner.work_deadline())
    }

    pub(in crate::node_runtime) fn cleanup_publication(&self) -> PublicationObservation {
        self.publication
    }

    pub(in crate::node_runtime) fn publish_cleanup(
        &mut self,
        section: &EmbeddedPublicationSection<'_>,
    ) -> Result<(), CompletePublicationError> {
        self.publish_cleanup_inner(
            section,
            #[cfg(test)]
            |_| Ok(()),
        )
    }

    #[cfg(test)]
    pub(in crate::node_runtime::pg_store) fn test_publish_cleanup(
        &mut self,
        section: &EmbeddedPublicationSection<'_>,
        hook: impl FnMut(&'static str) -> Result<(), StoreError>,
    ) -> Result<(), CompletePublicationError> {
        self.publish_cleanup_inner(section, hook)
    }

    #[cfg(test)]
    pub(in crate::node_runtime::pg_store) fn test_with_publication_permit<T>(
        &self,
        action: impl FnOnce(&AbandonedPartPublication<'_, '_>) -> T,
    ) -> T {
        action(&AbandonedPartPublication {
            source: &self.owner,
            candidate: &self.candidate,
            installation: &self.installation,
        })
    }

    fn publish_cleanup_inner(
        &mut self,
        section: &EmbeddedPublicationSection<'_>,
        #[cfg(test)] hook: impl FnMut(&'static str) -> Result<(), StoreError>,
    ) -> Result<(), CompletePublicationError> {
        let permit = AbandonedPartPublication {
            source: &self.owner,
            candidate: &self.candidate,
            installation: &self.installation,
        };
        permit.require_embedded_section(section)?;
        let sender = &self.owner.owner.owner;
        if self.publication == PublicationObservation::MarkerConfirmed {
            return Ok(());
        }
        // Record before the closed bridge, including panic/unwind. The bridge
        // can have committed even when this call returns an error. Installation
        // confirmation and the source's original history are never rewritten.
        self.publication = PublicationObservation::Unconfirmed;
        section.publish_abandoned_part_cleanup(
            &permit,
            sender.work_deadline(),
            #[cfg(test)]
            hook,
        )?;
        self.publication = PublicationObservation::MarkerConfirmed;
        Ok(())
    }
}

fn mismatch(operation: &'static str) -> StoreError {
    StoreError::RouteCapabilitySubjectMismatch { operation }
}

impl AbandonedPartPublication<'_, '_> {
    pub(in crate::node_runtime) fn inspect_custody_in_store(
        &self,
        pg: &PgStore,
        authority: &EmbeddedPublicationAuthority<'_>,
        request: &CompletePublicationRequest<'_>,
    ) -> Result<
        crate::node_runtime::pg_store::AbandonedPartCustodyObservation,
        crate::node_runtime::pg_store::AbandonedPartCustodyError,
    > {
        authority.require_abandoned_part_publication(self)?;
        authority.require_effect(pg, request)?;
        request.require_abandoned_part_binding(self)?;
        let observation = self
            .candidate
            .cleanup
            .inspect_abandoned_part_custody(pg, request.node_id().as_u32())?;
        authority.require_effect(pg, request)?;
        self.require_installed()?;
        Ok(observation)
    }

    pub(in crate::node_runtime) fn require_embedded_section(
        &self,
        section: &EmbeddedPublicationSection<'_>,
    ) -> Result<(), StoreError> {
        self.require_installed()?;
        AbandonedPartPreparation { owner: self.source }.require_embedded_section(section)
    }

    fn require_installed(&self) -> Result<(), StoreError> {
        self.source.require_valid()?;
        if *self.installation != AbandonedPartInstallationObservation::Installed {
            return Err(mismatch(
                "abandoned part publication requires confirmed installation",
            ));
        }
        Ok(())
    }

    pub(in crate::node_runtime::pg_store) fn require_member(
        &self,
        node: NodeId,
    ) -> Result<(), StoreError> {
        self.require_installed()?;
        if !self
            .source
            .owner
            .owner
            .route
            .nodes()
            .any(|member| member == node)
        {
            return Err(mismatch("abandoned part application original cohort"));
        }
        Ok(())
    }

    pub(in crate::node_runtime::pg_store) fn original_request(
        &self,
    ) -> Result<CompletePublicationRequest<'_>, StoreError> {
        self.require_installed()?;
        let sender = &self.source.owner.owner;
        sender
            .sender
            .source
            .request(sender.route.primary())
            .map_err(|_| mismatch("abandoned part publication original encoding"))
    }

    pub(in crate::node_runtime::pg_store) fn cleanup(
        &self,
    ) -> Result<EncodedContextualCommand<'_>, StoreError> {
        self.require_installed()?;
        self.candidate
            .cleanup
            .encode_command()
            .map_err(|_| mismatch("abandoned part publication cleanup encoding"))
    }

    pub(in crate::node_runtime) fn publish_in_store(
        &self,
        pg: &PgStore,
        authority: &EmbeddedPublicationAuthority<'_>,
        request: &CompletePublicationRequest<'_>,
        #[cfg(test)] hook: impl FnMut(&'static str) -> Result<(), StoreError>,
    ) -> Result<(), CompletePublicationError> {
        authority.require_abandoned_part_publication(self)?;
        authority.require_effect(pg, request)?;
        request.require_abandoned_part_binding(self)?;
        #[cfg(not(test))]
        request.start_embedded(pg, authority)?;
        #[cfg(test)]
        request.test_start_embedded_with_hook(pg, authority, hook)?;
        Ok(())
    }
}
