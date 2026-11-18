// Copyright The Argmin Authors.
// SPDX-License-Identifier: Apache-2.0

//! Introduction custody enters the existing file pipeline without reconstructing
//! command history. Preparation cannot write; dispatch consumes the exact guard.
use super::*;
use crate::cluster::EmbeddedPublicationSectionIdentity;
use crate::node_runtime::pg_store::payload_staging::DirectIntroductionFileScope;
use crate::node_runtime::pg_store::payload_staging::RemoteIntroductionFileScope;

#[must_use = "prepared files retain custody but cannot dispatch before section release"]
pub(in crate::node_runtime) struct DirectIntroductionFilePreparation<'route> {
    writer: DirectStagingFileWriter<'route>,
    section: EmbeddedPublicationSectionIdentity,
}

#[must_use = "failed release retains file custody and the supplied section"]
pub(in crate::node_runtime) struct DirectIntroductionFileReleaseFailure<'route, 'section> {
    prepared: Box<DirectIntroductionFilePreparation<'route>>,
    section: EmbeddedPublicationSection<'section>,
    error: Box<Error>,
}

impl CompletePublicationSender {
    pub(in crate::node_runtime::pg_store) fn bind_remote_introduction_files<'route>(
        scope: RemoteIntroductionFileScope<'route>,
        provisional: &crate::ObjectEncryption,
    ) -> Result<RemoteDirectStagingFileWriter<'route>, DirectStagingFileFailure<'route>> {
        let (route, sender, read, client, session) = scope.into_parts();
        let PublicationSource::Submitted { evidence, .. } = &sender.source else {
            unreachable!("remote introduction scope retains its submitted source")
        };
        let work = evidence.terminal_work();
        let slot_work = evidence.terminal_slot_work();
        let mut terminal = CompleteTerminalHandoff::from_confirmed_owner(
            CompleteFanoutSender { route, sender },
            work,
            slot_work,
        );
        terminal.retirement = TerminalSlotRetirementObservation::Retired;
        ConfirmedStagingWindow { terminal, read }.into_direct_file_writer_remote(
            client,
            session,
            provisional,
        )
    }

    pub(in crate::node_runtime::pg_store) fn prepare_introduction_files<'route>(
        scope: DirectIntroductionFileScope<'_, 'route>,
        provisional: &crate::ObjectEncryption,
    ) -> Result<DirectIntroductionFilePreparation<'route>, DirectStagingFileFailure<'route>> {
        let (route, sender, read, section, checked) = scope.into_parts();
        // The private scope can only contain a submitted, all-member-confirmed,
        // retired introduction. Keep that exact source and every journal entry.
        let PublicationSource::Submitted { evidence, .. } = &sender.source else {
            unreachable!("introduction scope retains its submitted source")
        };
        let work = evidence.terminal_work();
        let slot_work = evidence.terminal_slot_work();
        let mut terminal = CompleteTerminalHandoff::from_confirmed_owner(
            CompleteFanoutSender { route, sender },
            work,
            slot_work,
        );
        terminal.retirement = TerminalSlotRetirementObservation::Retired;
        let window = ConfirmedStagingWindow { terminal, read };
        if let Err(error) = checked {
            return Err(DirectStagingFileFailure {
                window: Box::new(window),
                error,
            });
        }
        let identity = section.introduction_identity();
        window
            .into_direct_file_writer_embedded(section, provisional)
            .map(|writer| DirectIntroductionFilePreparation {
                writer,
                section: identity,
            })
    }
}

impl<'route> DirectIntroductionFilePreparation<'route> {
    #[cfg(test)]
    pub(in crate::node_runtime::pg_store) fn test_original_deadlines(
        &self,
    ) -> (Instant, crate::storage_rpc::StorageRpcOperationDeadline) {
        let cutoff = self.writer.window.terminal.owner.captured_deadline();
        (cutoff.local, cutoff.portable)
    }

    #[cfg(test)]
    pub(in crate::node_runtime::pg_store) fn test_application(
        &self,
        node: crate::NodeId,
    ) -> ApplicationObservation {
        self.writer.window.terminal.application_observation(node)
    }
    pub(in crate::node_runtime) fn id(&self) -> MetadataCommandId {
        self.writer.window.id()
    }

    pub(in crate::node_runtime) fn release_primary<'section>(
        self,
        section: EmbeddedPublicationSection<'section>,
    ) -> Result<
        RemoteDirectStagingFileWriter<'route>,
        DirectIntroductionFileReleaseFailure<'route, 'section>,
    > {
        let checked = (|| {
            self.section.require_section(&section)?;
            section.require_effect()?;
            self.writer.window.require_valid()?;
            self.writer
                .fence
                .require_valid_for(self.id().cluster_epoch())?;
            Ok(())
        })();
        if let Err(error) = checked {
            return Err(DirectIntroductionFileReleaseFailure {
                prepared: Box::new(self),
                section,
                error: Box::new(error),
            });
        }
        // Synchronous guard drop precedes construction of the only type with
        // remote file effects. No callback, await, reconnect or effect in between.
        drop(section);
        Ok(self.writer.remote_dispatch_after_release())
    }

    #[cfg(test)]
    pub(in crate::node_runtime::pg_store) fn test_binding_state(
        &self,
    ) -> (Instant, Option<ContextSubmissionObservation>) {
        self.writer.test_binding_state()
    }
}

impl<'route, 'section> DirectIntroductionFileReleaseFailure<'route, 'section> {
    pub(in crate::node_runtime) fn error(&self) -> &Error {
        &self.error
    }

    pub(in crate::node_runtime) fn into_retry(
        self,
    ) -> (
        DirectIntroductionFilePreparation<'route>,
        EmbeddedPublicationSection<'section>,
    ) {
        (*self.prepared, self.section)
    }
}
