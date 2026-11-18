// Copyright The Argmin Authors.
// SPDX-License-Identifier: Apache-2.0

//! Fresh, admitted primary construction. No raw envelope/descriptor escapes;
//! capture is not acceptance, durable bucket-proof authorization or a file grant.
use super::*;
use crate::cluster::EmbeddedPublicationSection;
use crate::metadata_command::{
    MetadataCommandEnvelope, MetadataCommandLogIndex, MetadataCommandPayload,
    ReserveObjectGenerationCommand,
};
use crate::node_runtime::traits::PgMetadataStore;
use crate::storage_rpc::StorageRpcOperationDeadline;
use crate::types::AdmittedRouteEffectFence;
use crate::StoreError;
use std::time::Instant;

#[path = "payload_staging_direct_introduction_request.rs"]
mod request;
pub(crate) use request::DirectPutRequestCustody;

#[path = "payload_staging_direct_introduction_capture.rs"]
mod capture_core;
pub(in crate::node_runtime) use capture_core::CapturedDirectIntroductionCore;
#[path = "payload_staging_direct_introduction_receiver.rs"]
mod receiver;
#[path = "payload_staging_direct_introduction_transport.rs"]
mod transport;
pub(in crate::node_runtime) use receiver::{
    RemoteIntroductionInstallation, RemoteIntroductionInstallationState,
};
pub(in crate::node_runtime::pg_store) use transport::RemoteDirectIntroductionBinding;
pub(in crate::node_runtime::pg_store) use transport::RemoteIntroductionRetirementScope;
pub(in crate::node_runtime::pg_store) use transport::{
    RemoteIntroductionFileScope, RemoteIntroductionWindowScope,
};

#[path = "payload_staging_direct_introduction_publication.rs"]
mod publication;
pub(in crate::node_runtime::pg_store) use publication::DirectIntroductionFileScope;
pub(in crate::node_runtime::pg_store) use publication::DirectIntroductionRetirementScope;
pub(in crate::node_runtime::pg_store) use publication::DirectIntroductionWindowScope;

/// Historical acceptance observation, not a replication or physical-write grant.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(in crate::node_runtime) enum DirectIntroductionInstallation {
    NoAcceptanceObserved,
    Unconfirmed,
    Installed,
}

#[must_use]
pub(in crate::node_runtime) struct CapturedDirectIntroduction<'section, 'route> {
    section: &'section EmbeddedPublicationSection<'route>,
    cohort: crate::cluster::CompleteFanoutRoute<'route>,
    submission: submission::ContextualSubmission,
    fence: AdmittedRouteEffectFence,
    publication_deadline: Instant,
    publication_cutoff: StorageRpcOperationDeadline,
}

impl CapturedDirectIntroduction<'_, '_> {
    pub(in crate::node_runtime) fn id(&self) -> MetadataCommandId {
        self.submission.observation().command_id
    }

    pub(in crate::node_runtime) fn installation(&self) -> DirectIntroductionInstallation {
        match self.submission.observation().installation {
            submission::InstallationObservation::NoAcceptanceObserved => {
                DirectIntroductionInstallation::NoAcceptanceObserved
            }
            submission::InstallationObservation::Unconfirmed => {
                DirectIntroductionInstallation::Unconfirmed
            }
            submission::InstallationObservation::Installed => {
                DirectIntroductionInstallation::Installed
            }
        }
    }

    pub(in crate::node_runtime) fn install(
        &mut self,
    ) -> Result<(), StagingWindowConfirmationError> {
        self.section.install_direct_introduction(self)
    }

    pub(in crate::node_runtime) fn install_in_store(
        &mut self,
        authority: &crate::node_runtime::engine::DirectIntroductionAuthority<'_, '_, '_>,
        pg: &crate::node_runtime::pg_store::PgStore,
    ) -> Result<(), StagingWindowConfirmationError> {
        authority.require_store(pg)?;
        if !std::ptr::eq(authority.section(), self.section) {
            return Err(StoreError::RouteCapabilitySubjectMismatch {
                operation: "direct introduction original section",
            }
            .into());
        }
        self.require_valid()?;
        // This is an historical success observation, not a fresh inspection of
        // the slot. Never erase it on a subsequent failed/replaced-slot retry.
        if self.installation() == DirectIntroductionInstallation::Installed {
            return Ok(());
        }
        self.submission
            .install(
                pg,
                self.section.node_id().as_u32(),
                self.section.runtime()?.pg_topology(),
                self.fence,
                Some(self.section.deadline()),
            )
            .map_err(|error| StagingWindowConfirmationError::Window(error.to_string()))
    }

    // Never resample admission or expose independently installable context.
    fn require_valid(&self) -> Result<(), StoreError> {
        self.section.require_effect()?;
        self.fence.require_valid_for(self.section.epoch())
    }

    #[cfg(test)]
    pub(in crate::node_runtime::pg_store) fn test_original_deadlines(
        &self,
    ) -> (Instant, StorageRpcOperationDeadline) {
        (self.publication_deadline, self.publication_cutoff)
    }

    #[cfg(test)]
    pub(in crate::node_runtime::pg_store) fn test_submission(
        &self,
    ) -> &submission::ContextualSubmission {
        &self.submission
    }
}

impl<'section, 'route> CapturedDirectIntroduction<'section, 'route> {
    pub(in crate::node_runtime) fn capture(
        authority: &crate::node_runtime::engine::DirectIntroductionAuthority<'_, 'section, 'route>,
        pg: &crate::node_runtime::pg_store::PgStore,
        bucket: &BucketName,
        key: &ObjectKey,
        attempt: &SessionId,
        proof: &BucketWriteReservationProof,
        #[cfg(test)] mut hook: impl FnMut(&'static str),
    ) -> Result<CapturedDirectIntroduction<'section, 'route>, StagingWindowConfirmationError> {
        let section = authority.section();
        let cohort = section.introduction_cohort()?;
        let core = CapturedDirectIntroductionCore::embedded(
            authority,
            pg,
            bucket,
            key,
            attempt,
            proof,
            #[cfg(test)]
            &mut hook,
        )?;
        let captured = CapturedDirectIntroduction {
            section,
            cohort,
            submission: core.submission,
            fence: core.fence,
            publication_deadline: core.publication_deadline,
            publication_cutoff: core.publication_cutoff,
        };
        captured.require_valid()?;
        Ok(captured)
    }
}
