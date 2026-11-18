// Copyright The Argmin Authors.
// SPDX-License-Identifier: Apache-2.0

//! Candidate file binding. Direct PUT already owns its only VID;
//! binding does not allocate, write SQL, or add a registry. Keep complete custody
//! through every file attempt; neither errors nor Drop release the reservation.
//! A file acknowledgement is NOT durable acknowledgement-row publication.

use super::*;
use crate::types::AdmittedRouteEffectFence;
use crate::WriteAck;

#[path = "complete_direct_staging_files/introduction.rs"]
mod introduction;
pub(in crate::node_runtime) use introduction::DirectIntroductionFilePreparation;

#[path = "complete_direct_staging_files/handoff.rs"]
mod handoff;
pub(in crate::node_runtime) use handoff::{
    DirectStagingPreparation, DirectStagingSnapshot, DirectTerminalInstallation,
    DirectTerminalInstallationObservation, DirectTerminalInstallationProof,
    DirectTerminalPreparation,
};
#[path = "complete_direct_staging_files/remote.rs"]
mod remote;
use remote::FileAttempts;
pub(in crate::node_runtime) use remote::RemoteDirectFileAttempt;

#[must_use]
pub(in crate::node_runtime) struct DirectStagingFileWriter<'route> {
    window: ConfirmedStagingWindow<'route>,
    fence: AdmittedRouteEffectFence,
    deadline: Instant,
    acknowledgements: RecordedFileAcknowledgements,
    encryption_subject: crate::ObjectEncryptionSubject,
    provisional: crate::ObjectEncryption,
}

/// Only constructors that release the primary section can produce this type.
/// Ordinary embedded writers cannot substitute for released primary custody.
#[must_use]
pub(in crate::node_runtime) struct RemoteDirectStagingFileWriter<'route> {
    writer: DirectStagingFileWriter<'route>,
    cutoff: crate::storage_rpc::StorageRpcOperationDeadline,
    attempts: FileAttempts,
}

#[must_use]
pub(in crate::node_runtime) struct DirectStagingFileFailure<'route> {
    window: Box<ConfirmedStagingWindow<'route>>,
    error: Error,
}

impl<'route> ConfirmedStagingWindow<'route> {
    /// Consume original custody after one inspection on the already-held remote
    /// primary. No reconnect, fresh budget, or raw observation can mint a writer.
    /// Release acknowledgement precedes writer construction; the receiver must
    /// be able to acquire its own primary section before remote physical IO.
    /// Live publisher selection remains gated.
    pub(in crate::node_runtime) fn into_direct_file_writer_remote(
        self,
        client: &UnixStorageNodeClient,
        session: &UnixStorageNodeMetadataCommandSession,
        provisional: &crate::ObjectEncryption,
    ) -> Result<RemoteDirectStagingFileWriter<'route>, DirectStagingFileFailure<'route>> {
        let checked = (|| {
            self.require_valid()?;
            self.terminal.owner.require_primary(client, session)?;
            let deadline = self
                .read
                .deadline()
                .min(self.terminal.owner.work_deadline());
            let route = &self.terminal.owner.route;
            let mut held = session.borrow_complete_primary(
                client,
                self.id().pg_id(),
                route.epoch(),
                deadline,
            )?;
            self.read.inspect_direct_files_remote(
                &mut held,
                route.primary(),
                self.id().pg_id(),
                route.topology(),
            )?;
            self.require_valid()?;
            let fence = self.capture_file_fence_until(deadline)?;
            held.release_until()?;
            self.require_valid()?;
            fence.require_valid_for(route.epoch())?;
            Ok((fence, deadline))
        })();
        self.finish_direct_file_binding(checked, provisional)
            .and_then(DirectStagingFileWriter::into_remote_dispatch)
    }

    /// Consume the actual embedded guard, not a borrowed/scalar release claim.
    pub(in crate::node_runtime) fn into_direct_file_dispatch_embedded(
        self,
        section: EmbeddedPublicationSection<'_>,
        provisional: &crate::ObjectEncryption,
    ) -> Result<RemoteDirectStagingFileWriter<'route>, DirectStagingFileFailure<'route>> {
        let result = self.into_direct_file_writer_embedded(&section, provisional);
        drop(section);
        result.and_then(DirectStagingFileWriter::into_remote_dispatch)
    }

    pub(in crate::node_runtime) fn into_direct_file_writer_embedded(
        self,
        section: &EmbeddedPublicationSection<'_>,
        provisional: &crate::ObjectEncryption,
    ) -> Result<DirectStagingFileWriter<'route>, DirectStagingFileFailure<'route>> {
        let checked = (|| {
            self.require_valid()?;
            let route = &self.terminal.owner.route;
            route.require_embedded_section(section)?;
            section.require_direct_file_window(&self)?;
            section.require_effect()?;
            self.require_valid()?;
            Ok((
                self.capture_file_fence(section)?,
                self.file_deadline(section)?,
            ))
        })();
        self.finish_direct_file_binding(checked, provisional)
    }

    fn finish_direct_file_binding(
        self,
        checked: Result<(AdmittedRouteEffectFence, Instant), Error>,
        provisional: &crate::ObjectEncryption,
    ) -> Result<DirectStagingFileWriter<'route>, DirectStagingFileFailure<'route>> {
        let checked = checked.and_then(|(fence, deadline)| {
            let subject = self
                .read
                .direct_encryption_subject(self.terminal.owner.route.topology(), provisional)?;
            Ok((fence, deadline, subject))
        });
        match checked {
            Ok((fence, deadline, encryption_subject)) => Ok(DirectStagingFileWriter {
                window: self,
                fence,
                deadline,
                acknowledgements: RecordedFileAcknowledgements::default(),
                encryption_subject,
                provisional: provisional.clone(),
            }),
            Err(error) => Err(DirectStagingFileFailure {
                window: Box::new(self),
                error,
            }),
        }
    }

    // Shared by consuming file bindings, never a standalone grant.
    pub(super) fn capture_file_fence(
        &self,
        section: &EmbeddedPublicationSection<'_>,
    ) -> Result<AdmittedRouteEffectFence, Error> {
        let deadline = self.file_deadline(section)?;
        section.require_effect()?;
        self.capture_file_fence_until(deadline)
    }

    pub(super) fn capture_file_fence_until(
        &self,
        deadline: Instant,
    ) -> Result<AdmittedRouteEffectFence, Error> {
        self.require_valid()?;
        let route = &self.terminal.owner.route;
        let (_, route_fence) = route.staging_file_placement()?;
        let cutoff = crate::storage_rpc::StorageRpcOperationDeadline::from_instant(deadline);
        let fence = self
            .read
            .file_effect_fence()?
            .intersect(route_fence)?
            .intersect(AdmittedRouteEffectFence::bind_portable(
                route.epoch(),
                cutoff.portable_wall_valid_until_ms,
                cutoff.portable_wall_valid_until_ms,
            ))?;
        fence.require_valid_for(route.epoch())?;
        Ok(fence)
    }

    pub(in crate::node_runtime) fn require_direct_file_store(
        &self,
        section: &EmbeddedPublicationSection<'_>,
        pg: &crate::node_runtime::pg_store::PgStore,
    ) -> Result<(), Error> {
        self.require_valid()?;
        self.terminal
            .owner
            .route
            .require_embedded_section(section)?;
        self.read
            .require_direct_files(pg, section.node_id(), section.runtime()?.pg_topology())
    }

    pub(in crate::node_runtime) fn file_deadline(
        &self,
        section: &EmbeddedPublicationSection<'_>,
    ) -> Result<Instant, Error> {
        self.require_valid()?;
        self.terminal
            .owner
            .route
            .require_embedded_section(section)?;
        Ok(self
            .read
            .deadline()
            .min(section.deadline())
            .min(self.terminal.owner.work_deadline()))
    }
}

impl<'route> DirectStagingFileWriter<'route> {
    pub(in crate::node_runtime) fn encryption_subject(&self) -> &crate::ObjectEncryptionSubject {
        &self.encryption_subject
    }
    fn into_remote_dispatch(
        self,
    ) -> Result<RemoteDirectStagingFileWriter<'route>, DirectStagingFileFailure<'route>> {
        let checked = self.window.require_valid().and_then(|()| {
            self.fence
                .require_valid_for(self.window.terminal.owner.route.epoch())?;
            Ok(())
        });
        match checked {
            Ok(()) => Ok(self.remote_dispatch_after_release()),
            Err(error) => Err(DirectStagingFileFailure {
                window: Box::new(self.window),
                error,
            }),
        }
    }

    // Owner-subtree only. The consuming caller validates the original bounds
    // and releases the actual primary guard before entering here. Every effect
    // independently checks expiry; construction cannot renew the captured fence.
    fn remote_dispatch_after_release(self) -> RemoteDirectStagingFileWriter<'route> {
        let deadline = self.fence.deadline().expect("bounded confirmed window");
        let cutoff = crate::storage_rpc::StorageRpcOperationDeadline {
            portable_wall_valid_until_ms: deadline
                .portable_wall_valid_until_ms()
                .min(deadline.authority_valid_until_ms()),
        };
        RemoteDirectStagingFileWriter {
            writer: self,
            cutoff,
            attempts: FileAttempts::default(),
        }
    }
    #[cfg(test)]
    pub(in crate::node_runtime::pg_store) fn test_binding_state(
        &self,
    ) -> (Instant, Option<ContextSubmissionObservation>) {
        (self.deadline, self.window.installation_observation())
    }
    #[cfg(test)]
    pub(in crate::node_runtime) fn test_persist_acknowledgements_on_runtime(
        &self,
        runtime: &crate::node::LocalNodeRuntime,
    ) -> Result<(), Error> {
        self.acknowledgements
            .persist_on_runtime(&self.window, self.fence, self.deadline, runtime)
    }

    pub(in crate::node_runtime) fn id(&self) -> MetadataCommandId {
        self.window.id()
    }

    /// Exact retries reuse the fixed physical key. Never overwrite a different
    /// payload; the shared hard-link writer resolves concurrent publication.
    /// No caller can substitute a node, PG, VID, placement epoch or deadline.
    pub(in crate::node_runtime) fn write_embedded(
        &mut self,
        shard_index: u8,
        logical_size: u64,
        bytes: &[u8],
    ) -> Result<WriteAck, Error> {
        self.window.require_valid()?;
        let route = &self.window.terminal.owner.route;
        let (placement, _) = route.staging_file_placement()?;
        let (location, _) = self.window.read.direct_file_location(
            route.topology(),
            placement,
            shard_index,
            logical_size,
            bytes.len(),
        )?;
        let runtime = route.embedded_staging_file_runtime(&location)?;
        self.acknowledgements.bind_size(logical_size)?;
        // The existing primitive checks this captured intersection INSIDE the
        // shared file exclusion, before temporary/final publication, and holds
        // exclusion through sync and acknowledgement production. No PG mutex
        // or metadata RPC is acquired under that guard.
        let ack = runtime.write_direct_staging_file(self, shard_index, logical_size, bytes)?;
        self.acknowledgements.record(shard_index, ack);
        Ok(ack)
    }

    pub(in crate::node_runtime) fn persist_acknowledgements_embedded(
        &mut self,
    ) -> Result<(), Error> {
        self.acknowledgements
            .persist_embedded(&self.window, self.fence, self.deadline)
    }

    pub(in crate::node_runtime) fn require_file_runtime(
        &self,
        runtime: &crate::node::LocalNodeRuntime,
        shard_index: u8,
        logical_size: u64,
        bytes: usize,
    ) -> Result<
        (
            crate::cluster::ShardLocation,
            crate::ShardKey,
            AdmittedRouteEffectFence,
        ),
        Error,
    > {
        self.window.require_valid()?;
        let route = &self.window.terminal.owner.route;
        let (placement, _) = route.staging_file_placement()?;
        let (location, key) = self.window.read.direct_file_location(
            route.topology(),
            placement,
            shard_index,
            logical_size,
            bytes,
        )?;
        let expected = route.embedded_staging_file_runtime(&location)?;
        if !std::ptr::eq(runtime, expected) {
            return Err(Error::Window("direct file runtime domain".into()));
        }
        self.fence.require_valid_for(route.epoch())?;
        Ok((location, key, self.fence))
    }
}

impl RemoteDirectStagingFileWriter<'_> {
    #[cfg(test)]
    pub(in crate::node_runtime::pg_store) fn test_application(
        &self,
        node: crate::NodeId,
    ) -> ApplicationObservation {
        self.writer.window.terminal.application_observation(node)
    }
    #[cfg(test)]
    pub(in crate::node_runtime::pg_store) fn test_cutoff(&self) -> u64 {
        self.cutoff.portable_wall_valid_until_ms
    }
    pub(in crate::node_runtime) fn encryption_subject(&self) -> &crate::ObjectEncryptionSubject {
        self.writer.encryption_subject()
    }
    pub(in crate::node_runtime) fn id(&self) -> MetadataCommandId {
        self.writer.id()
    }
    #[cfg(test)]
    pub(in crate::node_runtime::pg_store) fn test_binding_state(
        &self,
    ) -> (Instant, Option<ContextSubmissionObservation>) {
        self.writer.test_binding_state()
    }
}

impl DirectStagingFileFailure<'_> {
    #[cfg(test)]
    pub(in crate::node_runtime::pg_store) fn test_binding_state(
        &self,
    ) -> (Instant, Option<ContextSubmissionObservation>) {
        (
            self.window.read.deadline(),
            self.window.installation_observation(),
        )
    }
    pub(in crate::node_runtime) fn id(&self) -> MetadataCommandId {
        self.window.id()
    }
    pub(in crate::node_runtime) fn error(&self) -> &Error {
        &self.error
    }
}
