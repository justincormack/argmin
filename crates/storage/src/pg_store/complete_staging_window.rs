// Copyright The Argmin Authors.
// SPDX-License-Identifier: Apache-2.0

//! One-way, no-effect handoff from retired all-member custody. This owns the
//! complete context/history and fresh window read together, without a raw
//! descriptor, reverse conversion or renewal API. Allocation and file/ack
//! effect admission remain separate gates; this is NOT a physical-write grant.

use super::*;
use crate::node_runtime::pg_store::payload_staging::CapturedWindowRead;
use crate::node_runtime::pg_store::{
    StagingWindowConfirmationError as Error, StagingWindowConfirmationObservation,
};
pub(in crate::node_runtime) use allocation::{
    StreamAppendInstallation, StreamAppendInstallationObservation, StreamAppendInstallationProof,
    StreamAppendPreparation,
};
pub(in crate::node_runtime) use files::{
    DirectIntroductionFilePreparation, DirectStagingFileFailure,
};

#[path = "complete_staging_acknowledgements.rs"]
mod acknowledgements;
use acknowledgements::{
    RecordedFileAcknowledgements, RemoteAcknowledgementScope, StagingFileHandoffFailure,
};
pub(in crate::node_runtime) use acknowledgements::{
    RemoteStagingAcknowledgementAttempt, StagingAcknowledgementBatch,
};

#[path = "complete_direct_staging_files.rs"]
mod files;
pub(in crate::node_runtime) use files::RemoteDirectStagingFileWriter;
pub(in crate::node_runtime) use files::{DirectStagingFileWriter, RemoteDirectFileAttempt};
pub(in crate::node_runtime) use files::{
    DirectStagingPreparation, DirectStagingSnapshot, DirectTerminalInstallation,
    DirectTerminalInstallationObservation, DirectTerminalInstallationProof,
    DirectTerminalPreparation,
};

#[path = "complete_stream_window_allocation.rs"]
mod allocation;
#[cfg(test)]
pub(in crate::node_runtime) use allocation::RemoteStreamStagingFileWriter;
pub(in crate::node_runtime) use allocation::{
    AllocatedStreamWindow, RemoteStreamAllocationAttempt, RemoteStreamFileAttempt,
    StreamStagingFileWriter, StreamWindowAllocationFailure, StreamWindowAllocationPermit,
};

#[cfg(test)]
#[path = "complete_staging_window/api_tests.rs"]
mod api_tests;

#[must_use = "retains original command custody; not physical-write permission"]
pub(in crate::node_runtime) struct ConfirmedStagingWindow<'route> {
    terminal: CompleteTerminalHandoff<'route>,
    read: CapturedWindowRead,
}

#[must_use = "failed confirmation retains the original terminal holder"]
pub(in crate::node_runtime) struct StagingWindowHandoffFailure<'route> {
    terminal: Box<CompleteTerminalHandoff<'route>>,
    error: Error,
}

impl CompletePublicationSender {
    pub(in crate::node_runtime::pg_store) fn capture_remote_direct_introduction_window(
        scope: crate::node_runtime::pg_store::payload_staging::RemoteIntroductionWindowScope<
            '_,
            '_,
        >,
        #[cfg(test)] hook: impl FnMut(&'static str),
    ) -> Result<CapturedWindowRead, Error> {
        let (sender, route, client, session, deadline) = scope.into_parts();
        let PublicationSource::Submitted { evidence, .. } = &sender.source else {
            return Err(Error::Window(
                "remote introduction window requires original submission".into(),
            ));
        };
        evidence.capture_window_read_remote(
            route,
            client,
            session,
            deadline,
            #[cfg(test)]
            hook,
        )
    }

    pub(in crate::node_runtime::pg_store) fn capture_direct_introduction_window(
        scope: crate::node_runtime::pg_store::payload_staging::DirectIntroductionWindowScope<
            '_,
            '_,
            '_,
        >,
        #[cfg(test)] hook: impl FnMut(&'static str),
    ) -> Result<CapturedWindowRead, Error> {
        let (sender, section, deadline) = scope.into_parts();
        // An introduction can only originate in the admitted fresh builder.
        // Never substitute inspected command bytes or resample a new budget.
        let PublicationSource::Submitted { evidence, .. } = &sender.source else {
            return Err(Error::Window(
                "introduction window requires original submission".into(),
            ));
        };
        evidence.capture_window_read_embedded(
            section,
            deadline,
            #[cfg(test)]
            hook,
        )
    }
}

impl<'route> CompleteTerminalHandoff<'route> {
    pub(in crate::node_runtime) fn into_confirmed_window_embedded(
        self,
        section: &EmbeddedPublicationSection<'_>,
        deadline: Instant,
        #[cfg(test)] hook: impl FnMut(&'static str),
    ) -> Result<ConfirmedStagingWindow<'route>, StagingWindowHandoffFailure<'route>> {
        let result = (|| {
            self.require_window_handoff()?;
            self.owner.route.require_embedded_section(section)?;
            let deadline = deadline
                .min(self.owner.work_deadline())
                .min(section.deadline());
            match &self.owner.sender.source {
                PublicationSource::Inspected(evidence) => evidence
                    .pending
                    .as_ref()
                    .expect("terminal owns command")
                    .0
                    .capture_window_read_embedded(
                        section,
                        deadline,
                        #[cfg(test)]
                        hook,
                    ),
                PublicationSource::Submitted { evidence, .. } => evidence
                    .capture_window_read_embedded(
                        section,
                        deadline,
                        #[cfg(test)]
                        hook,
                    ),
            }
        })();
        self.finish_window_handoff(result)
    }

    pub(in crate::node_runtime) fn into_confirmed_window_remote(
        self,
        client: &UnixStorageNodeClient,
        session: &UnixStorageNodeMetadataCommandSession,
        deadline: Instant,
        #[cfg(test)] hook: impl FnMut(&'static str),
    ) -> Result<ConfirmedStagingWindow<'route>, StagingWindowHandoffFailure<'route>> {
        let result = (|| {
            self.require_window_handoff()?;
            self.owner.require_primary(client, session)?;
            let deadline = self
                .owner
                .route
                .window_confirmation_deadline(client, deadline.min(self.owner.work_deadline()))?;
            match &self.owner.sender.source {
                PublicationSource::Inspected(evidence) => evidence
                    .pending
                    .as_ref()
                    .expect("terminal owns command")
                    .0
                    .capture_window_read_remote(
                        &self.owner.route,
                        client,
                        session,
                        deadline,
                        #[cfg(test)]
                        hook,
                    ),
                PublicationSource::Submitted { evidence, .. } => evidence
                    .capture_window_read_remote(
                        &self.owner.route,
                        client,
                        session,
                        deadline,
                        #[cfg(test)]
                        hook,
                    ),
            }
        })();
        self.finish_window_handoff(result)
    }

    fn require_window_handoff(&self) -> Result<(), Error> {
        self.require_valid()?;
        if self.owner.route.is_historical() {
            return Err(Error::Window(
                "window handoff requires current admission".into(),
            ));
        }
        if self.retirement != TerminalSlotRetirementObservation::Retired {
            return Err(Error::Window(
                "window handoff requires confirmed slot retirement".into(),
            ));
        }
        if crate::clock::current_time_millis()
            >= self
                .owner
                .captured_deadline()
                .portable
                .portable_wall_valid_until_ms
        {
            return Err(StoreError::OperationDeadlineExceeded {
                context: "window handoff original publication cutoff",
            }
            .into());
        }
        Ok(())
    }

    fn finish_window_handoff(
        self,
        read: Result<CapturedWindowRead, Error>,
    ) -> Result<ConfirmedStagingWindow<'route>, StagingWindowHandoffFailure<'route>> {
        // No new admission or clock-derived budget between inspection and minting.
        let result = read.and_then(|read| {
            self.require_window_handoff()?;
            read.require_valid()?;
            Ok(read)
        });
        match result {
            Ok(read) => Ok(ConfirmedStagingWindow {
                terminal: self,
                read,
            }),
            Err(error) => Err(StagingWindowHandoffFailure {
                terminal: Box::new(self),
                error,
            }),
        }
    }
}

impl ConfirmedStagingWindow<'_> {
    pub(in crate::node_runtime) fn id(&self) -> MetadataCommandId {
        self.terminal.id()
    }

    pub(in crate::node_runtime) fn observation(&self) -> &StagingWindowConfirmationObservation {
        self.read.observation()
    }

    /// Time/admission validity only, not a new settled-prefix observation,
    /// permission to allocate, or local write/cleanup exclusion.
    pub(in crate::node_runtime) fn require_valid(&self) -> Result<(), Error> {
        self.terminal.require_window_handoff()?;
        self.read.require_valid()
    }

    #[cfg(test)]
    pub(in crate::node_runtime::pg_store) fn installation_observation(
        &self,
    ) -> Option<ContextSubmissionObservation> {
        self.terminal.installation_observation()
    }
}

impl<'route> StagingWindowHandoffFailure<'route> {
    pub(in crate::node_runtime) fn error(&self) -> &Error {
        &self.error
    }
    pub(in crate::node_runtime) fn into_terminal(self) -> CompleteTerminalHandoff<'route> {
        *self.terminal
    }
}
