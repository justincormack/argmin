// Copyright The Argmin Authors.
// SPDX-License-Identifier: Apache-2.0

//! Only the held admitted section selects the actual primary store.
use super::*;
use crate::cluster::EmbeddedPublicationSection;
use crate::node_runtime::pg_store::{CapturedDirectIntroduction, StagingWindowConfirmationError};
use crate::{BucketWriteReservationProof, SessionId};

pub(in crate::node_runtime) struct DirectIntroductionAuthority<'store, 'section, 'route> {
    section: &'section EmbeddedPublicationSection<'route>,
    store: &'store PgStore,
}

impl<'section, 'route> DirectIntroductionAuthority<'_, 'section, 'route> {
    pub(in crate::node_runtime) fn section(&self) -> &'section EmbeddedPublicationSection<'route> {
        self.section
    }
    pub(in crate::node_runtime) fn require_store(&self, pg: &PgStore) -> Result<(), StoreError> {
        self.section.require_effect()?;
        self.section.staging_placement()?;
        if !std::ptr::eq(pg, self.store) || pg.pg_id() != self.section.pg_id().get() {
            return Err(StoreError::RouteCapabilitySubjectMismatch {
                operation: "direct introduction admitted primary store",
            });
        }
        Ok(())
    }
}

impl<'route> EmbeddedPublicationSection<'route> {
    pub(in crate::node_runtime) fn install_direct_introduction(
        &self,
        captured: &mut CapturedDirectIntroduction<'_, '_>,
    ) -> Result<(), StagingWindowConfirmationError> {
        self.require_effect()?;
        self.staging_placement()?;
        let runtime = self.runtime()?;
        let pg = runtime
            .node
            .get_pg_until(self.pg_id().get(), self.deadline())?;
        captured.install_in_store(
            &DirectIntroductionAuthority {
                section: self,
                store: &pg,
            },
            &pg,
        )
    }

    #[cfg(test)]
    pub(in crate::node_runtime) fn test_direct_introduction_store_binding(
        &self,
        foreign: &PgStore,
    ) -> Result<(), StoreError> {
        let runtime = self.runtime()?;
        let pg = runtime
            .node
            .get_pg_until(self.pg_id().get(), self.deadline())?;
        DirectIntroductionAuthority {
            section: self,
            store: &pg,
        }
        .require_store(foreign)
    }

    pub(in crate::node_runtime) fn prepare_direct_introduction<'section>(
        &'section self,
        bucket: &BucketName,
        key: &ObjectKey,
        attempt: &SessionId,
        proof: &BucketWriteReservationProof,
    ) -> Result<CapturedDirectIntroduction<'section, 'route>, StagingWindowConfirmationError> {
        self.prepare_direct_introduction_inner(
            bucket,
            key,
            attempt,
            proof,
            #[cfg(test)]
            |_| {},
        )
    }

    fn prepare_direct_introduction_inner<'section>(
        &'section self,
        bucket: &BucketName,
        key: &ObjectKey,
        attempt: &SessionId,
        proof: &BucketWriteReservationProof,
        #[cfg(test)] hook: impl FnMut(&'static str),
    ) -> Result<CapturedDirectIntroduction<'section, 'route>, StagingWindowConfirmationError> {
        self.require_effect()?;
        self.staging_placement()?; // Reject historical sections before PG access.
        let runtime = self.runtime()?;
        let pg = runtime
            .node
            .get_pg_until(self.pg_id().get(), self.deadline())?;
        let authority = DirectIntroductionAuthority {
            section: self,
            store: &pg,
        };
        CapturedDirectIntroduction::capture(
            &authority,
            &pg,
            bucket,
            key,
            attempt,
            proof,
            #[cfg(test)]
            hook,
        )
    }

    #[cfg(test)]
    pub(in crate::node_runtime) fn test_prepare_direct_introduction<'section>(
        &'section self,
        bucket: &BucketName,
        key: &ObjectKey,
        attempt: &SessionId,
        proof: &BucketWriteReservationProof,
        hook: impl FnMut(&'static str),
    ) -> Result<CapturedDirectIntroduction<'section, 'route>, StagingWindowConfirmationError> {
        self.prepare_direct_introduction_inner(bucket, key, attempt, proof, hook)
    }
}
