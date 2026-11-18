// Copyright The Argmin Authors.
// SPDX-License-Identifier: Apache-2.0

//! Storage-owned bridge from a real admitted embedded primary section to the
//! complete publication transaction. Constructors remain private to this owner.

use super::*;
use crate::cluster::EmbeddedPublicationSection;
use crate::node_runtime::pg_store::{
    AbandonedPartInstallation, AbandonedPartPreparation, AbandonedPartPublication,
    CompletePublicationError, CompletePublicationRequest, PreparedAbandonedPartBorrow,
    TerminalSlotRetirementAuthority, UnstartedAbandonmentAuthority,
};
use std::time::Instant;

pub(in crate::node_runtime) struct EmbeddedStagingAckAuthority<'a, 'b, 'route> {
    runtime: &'a LocalNodeRuntime,
    store: &'a PgStore,
    batch: &'a crate::node_runtime::pg_store::StagingAcknowledgementBatch<'b, 'route>,
}

impl EmbeddedStagingAckAuthority<'_, '_, '_> {
    pub(in crate::node_runtime) fn require(
        &self,
        batch: &crate::node_runtime::pg_store::StagingAcknowledgementBatch<'_, '_>,
        store: &PgStore,
    ) -> Result<(), crate::node_runtime::pg_store::StagingWindowConfirmationError> {
        let (pg, _) = batch.require_runtime(self.runtime)?;
        if !std::ptr::eq(store, self.store)
            || !std::ptr::eq(batch, self.batch)
            || pg.get() != store.pg_id()
        {
            return Err(StoreError::RouteCapabilitySubjectMismatch {
                operation: "staging acknowledgement admitted store/batch",
            }
            .into());
        }
        Ok(())
    }
}

#[cfg(test)]
#[path = "complete_publication/api_tests.rs"]
mod api_tests;

pub(in crate::node_runtime) struct EmbeddedPublicationAuthority<'a> {
    section: &'a EmbeddedPublicationSection<'a>,
    store: &'a PgStore,
    request: &'a CompletePublicationRequest<'a>,
    deadline: Instant,
}

impl EmbeddedPublicationAuthority<'_> {
    pub(in crate::node_runtime) fn direct_reinspection_bounds(
        &self,
        permit: &crate::node_runtime::pg_store::DirectStagingPreparation<'_, '_>,
    ) -> Result<
        (Instant, crate::types::AdmittedRouteEffectFence),
        crate::node_runtime::pg_store::StagingWindowConfirmationError,
    > {
        permit.require_section(self.section)?;
        Ok((self.deadline, self.section.staging_capture_fence()?))
    }
    pub(in crate::node_runtime) fn direct_terminal_installation_deadline(
        &self,
        proof: &crate::node_runtime::pg_store::DirectTerminalInstallationProof<'_, '_>,
    ) -> Result<Instant, crate::node_runtime::pg_store::StagingWindowConfirmationError> {
        proof.require_section(self.section)?;
        Ok(self.deadline)
    }
    pub(in crate::node_runtime) fn require_direct_terminal(
        &self,
        permit: &crate::node_runtime::pg_store::DirectTerminalPreparation<'_, '_>,
    ) -> Result<(), crate::node_runtime::pg_store::StagingWindowConfirmationError> {
        permit.require_section(self.section)
    }
    pub(in crate::node_runtime) fn require_direct_preparation(
        &self,
        permit: &crate::node_runtime::pg_store::DirectStagingPreparation<'_, '_>,
    ) -> Result<(), crate::node_runtime::pg_store::StagingWindowConfirmationError> {
        permit.require_section(self.section)
    }
    pub(in crate::node_runtime) fn stream_append_installation_deadline(
        &self,
        proof: &crate::node_runtime::pg_store::StreamAppendInstallationProof<'_, '_>,
    ) -> Result<Instant, crate::node_runtime::pg_store::StagingWindowConfirmationError> {
        proof.require_section(self.section)?;
        Ok(self.deadline)
    }
    pub(in crate::node_runtime) fn require_stream_append_preparation(
        &self,
        permit: &crate::node_runtime::pg_store::StreamAppendPreparation<'_, '_>,
    ) -> Result<(), crate::node_runtime::pg_store::StagingWindowConfirmationError> {
        permit.require_section(self.section)
    }
    pub(in crate::node_runtime) fn require_stream_window_allocation(
        &self,
        permit: &crate::node_runtime::pg_store::StreamWindowAllocationPermit<'_, '_>,
    ) -> Result<(), crate::node_runtime::pg_store::StagingWindowConfirmationError> {
        permit.require_section(self.section)
    }
    pub(in crate::node_runtime) fn require_abandoned_part_publication(
        &self,
        permit: &AbandonedPartPublication<'_, '_>,
    ) -> Result<(), StoreError> {
        permit.require_embedded_section(self.section)
    }

    pub(in crate::node_runtime) fn require_abandoned_part_installation(
        &self,
        permit: &AbandonedPartInstallation<'_, '_>,
    ) -> Result<(), StoreError> {
        permit.require_embedded_section(self.section)
    }
    pub(in crate::node_runtime) fn require_abandoned_part_preparation(
        &self,
        permit: &AbandonedPartPreparation<'_, '_>,
    ) -> Result<(), StoreError> {
        permit.require_embedded_section(self.section)
    }

    pub(in crate::node_runtime) fn require_effect(
        &self,
        store: &PgStore,
        request: &CompletePublicationRequest<'_>,
    ) -> Result<(), StoreError> {
        self.section.require_effect()?;
        self.section.require_command(
            request.id(),
            request.checksum_crc64(),
            request.command_sha256(),
        )?;
        if !std::ptr::eq(store, self.store)
            || store.pg_id() != self.section.pg_id().get()
            || request.node_id() != self.section.node_id()
            || request.id().pg_id() != self.section.pg_id()
            || request.id().cluster_epoch() != self.section.epoch()
            || request.id() != self.request.id()
            || request.command_sha256() != self.request.command_sha256()
            || request.topology() != self.section.runtime()?.pg_topology()
        {
            return Err(StoreError::RouteCapabilitySubjectMismatch {
                operation: "embedded complete publication admitted command/store/primary",
            });
        }
        if Instant::now() >= request.bound_deadline(self.deadline) {
            return Err(StoreError::OperationDeadlineExceeded {
                context: "embedded complete publication start",
            });
        }
        Ok(())
    }
}

impl EmbeddedPublicationSection<'_> {
    pub(in crate::node_runtime) fn install_direct_terminal(
        &self,
        request: &CompletePublicationRequest<'_>,
        attempt: crate::node_runtime::pg_store::DirectTerminalInstallation<'_, '_>,
    ) -> Result<(), crate::node_runtime::pg_store::StagingWindowConfirmationError> {
        attempt.require_section(self)?;
        self.with_authority(request, attempt.deadline(), |pg, authority| {
            attempt.execute_in_store(pg, authority, request)
        })
    }
    pub(in crate::node_runtime) fn prepare_direct_terminal(
        &self,
        permit: &crate::node_runtime::pg_store::DirectTerminalPreparation<'_, '_>,
    ) -> Result<
        crate::node_runtime::pg_store::CapturedDirectTerminal,
        crate::node_runtime::pg_store::StagingWindowConfirmationError,
    > {
        permit.require_section(self)?;
        let request = permit.publication_request()?;
        self.with_authority(&request, permit.deadline(), |pg, authority| {
            permit.capture_in_store(pg, authority, &request)
        })
    }
    pub(in crate::node_runtime) fn inspect_direct_contender(
        &self,
        permit: &crate::node_runtime::pg_store::DirectStagingPreparation<'_, '_>,
    ) -> Result<
        crate::node_runtime::pg_store::CompletePendingEvidence,
        crate::node_runtime::pg_store::StagingWindowConfirmationError,
    > {
        permit.require_section(self)?;
        let request = permit.publication_request()?;
        self.with_authority(&request, permit.deadline(), |pg, authority| {
            permit.capture_contender_in_store(pg, authority, &request)
        })
    }
    pub(in crate::node_runtime) fn prepare_acknowledged_direct_payload(
        &self,
        permit: &crate::node_runtime::pg_store::DirectStagingPreparation<'_, '_>,
    ) -> Result<
        crate::node_runtime::pg_store::DirectStagingSnapshot,
        crate::node_runtime::pg_store::StagingWindowConfirmationError,
    > {
        permit.require_section(self)?;
        let request = permit.publication_request()?;
        self.with_authority(&request, permit.deadline(), |pg, authority| {
            permit.capture_in_store(pg, authority, &request)
        })
    }
    pub(in crate::node_runtime) fn install_acknowledged_stream_append(
        &self,
        request: &CompletePublicationRequest<'_>,
        attempt: crate::node_runtime::pg_store::StreamAppendInstallation<'_, '_>,
    ) -> Result<(), crate::node_runtime::pg_store::StagingWindowConfirmationError> {
        attempt.require_section(self)?;
        self.with_authority(request, attempt.deadline(), |pg, authority| {
            attempt.execute_in_store(pg, authority, request)
        })
    }
    pub(in crate::node_runtime) fn prepare_acknowledged_stream_append(
        &self,
        permit: &crate::node_runtime::pg_store::StreamAppendPreparation<'_, '_>,
    ) -> Result<
        crate::node_runtime::pg_store::CapturedStreamAppend,
        crate::node_runtime::pg_store::StagingWindowConfirmationError,
    > {
        permit.require_section(self)?;
        let request = permit.publication_request()?;
        let placement = self.staging_placement()?;
        self.with_authority(&request, permit.deadline(), |pg, authority| {
            permit.prepare_in_store(pg, authority, &request, placement)
        })
    }
    pub(in crate::node_runtime) fn require_stream_file_window(
        &self,
        allocated: &crate::node_runtime::pg_store::AllocatedStreamWindow<'_>,
    ) -> Result<(), crate::node_runtime::pg_store::StagingWindowConfirmationError> {
        self.require_effect()?;
        let runtime = self.runtime()?;
        let pg = runtime
            .node
            .get_pg_until(self.pg_id().get(), allocated.file_deadline(self)?)?;
        self.require_effect()?;
        let snapshot = pg.inspect_complete_settled_prefix(
            self.node_id().as_u32(),
            self.epoch(),
            runtime.pg_topology(),
        )?;
        allocated.require_file_store(self, &pg)?;
        snapshot.finish()?;
        self.require_effect()?;
        Ok(())
    }

    pub(in crate::node_runtime) fn require_direct_file_window(
        &self,
        window: &crate::node_runtime::pg_store::ConfirmedStagingWindow<'_>,
    ) -> Result<(), crate::node_runtime::pg_store::StagingWindowConfirmationError> {
        self.require_effect()?;
        let runtime = self.runtime()?;
        let pg = runtime
            .node
            .get_pg_until(self.pg_id().get(), window.file_deadline(self)?)?;
        self.require_effect()?;
        let snapshot = pg.inspect_complete_settled_prefix(
            self.node_id().as_u32(),
            self.epoch(),
            runtime.pg_topology(),
        )?;
        window.require_direct_file_store(self, &pg)?;
        snapshot.finish()?;
        self.require_effect()?;
        Ok(())
    }

    pub(in crate::node_runtime) fn allocate_confirmed_stream_window(
        &self,
        permit: &crate::node_runtime::pg_store::StreamWindowAllocationPermit<'_, '_>,
        #[cfg(test)] hook: impl FnMut(&'static str) -> Result<(), StoreError>,
    ) -> Result<
        crate::StreamUploadSegmentRecord,
        crate::node_runtime::pg_store::StagingWindowConfirmationError,
    > {
        permit.require_section(self)?;
        let request = permit.publication_request()?;
        let placement = self.staging_placement()?;
        self.with_authority(&request, permit.deadline(), |pg, authority| {
            pg.allocate_confirmed_stream_window(
                permit,
                authority,
                &request,
                placement,
                #[cfg(test)]
                hook,
            )
        })
    }
    pub(in crate::node_runtime) fn inspect_abandoned_part_custody(
        &self,
        permit: &AbandonedPartPublication<'_, '_>,
        deadline: Instant,
    ) -> Result<
        crate::node_runtime::pg_store::AbandonedPartCustodyObservation,
        crate::node_runtime::pg_store::AbandonedPartCustodyError,
    > {
        permit.require_embedded_section(self)?;
        let request = CompletePublicationRequest::abandoned_part_cleanup_request(permit)?;
        self.with_authority(&request, deadline, |pg, authority| {
            permit.inspect_custody_in_store(pg, authority, &request)
        })
    }

    pub(in crate::node_runtime) fn publish_abandoned_part_cleanup(
        &self,
        permit: &AbandonedPartPublication<'_, '_>,
        deadline: Instant,
        #[cfg(test)] hook: impl FnMut(&'static str) -> Result<(), StoreError>,
    ) -> Result<(), CompletePublicationError> {
        permit.require_embedded_section(self)?;
        let request = CompletePublicationRequest::abandoned_part_cleanup_request(permit)?;
        self.with_authority(&request, deadline, |pg, authority| {
            permit.publish_in_store(
                pg,
                authority,
                &request,
                #[cfg(test)]
                hook,
            )
        })
    }

    pub(in crate::node_runtime) fn install_abandoned_part_cleanup(
        &self,
        request: &CompletePublicationRequest<'_>,
        attempt: AbandonedPartInstallation<'_, '_>,
        deadline: Instant,
        #[cfg(test)] hook: impl FnMut(&'static str) -> Result<(), StoreError>,
    ) -> Result<(), CompletePublicationError> {
        attempt.require_embedded_section(self)?;
        attempt.require_request(request)?;
        self.with_authority(request, deadline, |pg, authority| {
            attempt.execute_in_store(
                pg,
                authority,
                request,
                #[cfg(test)]
                hook,
            )
        })
    }

    pub(in crate::node_runtime) fn prepare_abandoned_part_cleanup<'a>(
        &self,
        request: &CompletePublicationRequest<'_>,
        permit: &'a AbandonedPartPreparation<'_, '_>,
        deadline: Instant,
        #[cfg(test)] hook: impl FnMut(&'static str) -> Result<(), StoreError>,
    ) -> Result<
        PreparedAbandonedPartBorrow<'a>,
        crate::node_runtime::pg_store::AbandonedPartPreparationError,
    > {
        permit.require_embedded_section(self)?;
        self.with_authority(request, deadline, |pg, authority| {
            permit.prepare_in_store(
                pg,
                authority,
                request,
                #[cfg(test)]
                hook,
            )
        })
    }

    pub(in crate::node_runtime) fn abandon_unstarted_complete_publication(
        &self,
        request: &CompletePublicationRequest<'_>,
        custody: &UnstartedAbandonmentAuthority<'_, '_>,
        deadline: Instant,
        #[cfg(test)] hook: impl FnMut(&'static str) -> Result<(), StoreError>,
    ) -> Result<(), CompletePublicationError> {
        custody.require_embedded_section(self)?;
        self.with_authority(request, deadline, |pg, authority| {
            request.abandon_unstarted_embedded(
                pg,
                authority,
                custody,
                #[cfg(test)]
                hook,
            )
        })
    }

    // Publication authority alone cannot retire a slot: only the owning,
    // all-member terminal holder can lend the additional stack-only permit.
    pub(in crate::node_runtime) fn retire_complete_publication(
        &self,
        request: &CompletePublicationRequest<'_>,
        terminal: &TerminalSlotRetirementAuthority<'_>,
        deadline: Instant,
        #[cfg(test)] hook: impl FnMut(&'static str) -> Result<(), StoreError>,
    ) -> Result<(), CompletePublicationError> {
        terminal.require_embedded_section(self)?;
        self.with_authority(request, deadline, |pg, authority| {
            request.retire_embedded(
                pg,
                authority,
                terminal,
                #[cfg(test)]
                hook,
            )
        })
    }

    pub(in crate::node_runtime) fn start_complete_publication(
        &self,
        request: &CompletePublicationRequest<'_>,
        deadline: Instant,
    ) -> Result<(), CompletePublicationError> {
        self.with_authority(request, deadline, |pg, authority| {
            request.start_embedded(pg, authority)
        })
    }

    // Private: production callers cannot inject a mutation callback or supply a
    // different store/runtime. The selected runtime itself opens the PG mutex.
    fn with_authority<T, E: From<StoreError>>(
        &self,
        request: &CompletePublicationRequest<'_>,
        deadline: Instant,
        action: impl FnOnce(&PgStore, &EmbeddedPublicationAuthority<'_>) -> Result<T, E>,
    ) -> Result<T, E> {
        let deadline = request.bound_deadline(deadline.min(self.deadline()));
        self.require_effect()?;
        let runtime = self.runtime()?;
        let pg = runtime.node.get_pg_until(self.pg_id().get(), deadline)?;
        let authority = EmbeddedPublicationAuthority {
            section: self,
            store: &pg,
            request,
            deadline,
        };
        authority.require_effect(&pg, request)?;
        action(&pg, &authority)
    }

    #[cfg(test)]
    pub(in crate::node_runtime) fn test_with_publication_authority<T>(
        &self,
        request: &CompletePublicationRequest<'_>,
        deadline: Instant,
        action: impl FnOnce(
            &PgStore,
            &EmbeddedPublicationAuthority<'_>,
        ) -> Result<T, CompletePublicationError>,
    ) -> Result<T, CompletePublicationError> {
        self.with_authority(request, deadline, action)
    }
}

impl LocalNodeRuntime {
    pub(in crate::node_runtime) fn persist_staging_acknowledgements(
        &self,
        batch: &crate::node_runtime::pg_store::StagingAcknowledgementBatch<'_, '_>,
    ) -> Result<(), crate::node_runtime::pg_store::StagingWindowConfirmationError> {
        let (pg_id, deadline) = batch.require_runtime(self)?;
        let pg = self.node.get_pg_until(pg_id.get(), deadline)?;
        batch.apply_in_store(
            &pg,
            &EmbeddedStagingAckAuthority {
                runtime: self,
                store: &pg,
                batch,
            },
        )
    }

    pub(in crate::node_runtime) fn write_stream_staging_file(
        &self,
        writer: &crate::node_runtime::pg_store::StreamStagingFileWriter<'_>,
        shard_index: u8,
        bytes: &[u8],
    ) -> Result<WriteAck, crate::node_runtime::pg_store::StagingWindowConfirmationError> {
        let (location, key, fence) = writer.require_file_runtime(self, shard_index, bytes.len())?;
        Ok(self.node.write_shard_file_if_absent_with_effect_fence(
            location.data_pg_id().get(),
            &key,
            bytes,
            fence,
        )?)
    }

    pub(in crate::node_runtime) fn write_direct_staging_file(
        &self,
        writer: &crate::node_runtime::pg_store::DirectStagingFileWriter<'_>,
        shard_index: u8,
        logical_size: u64,
        bytes: &[u8],
    ) -> Result<WriteAck, crate::node_runtime::pg_store::StagingWindowConfirmationError> {
        let (location, key, fence) =
            writer.require_file_runtime(self, shard_index, logical_size, bytes.len())?;
        Ok(self.node.write_shard_file_if_absent_with_effect_fence(
            location.data_pg_id().get(),
            &key,
            bytes,
            fence,
        )?)
    }
}
