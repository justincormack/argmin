// Copyright The Argmin Authors.
// SPDX-License-Identifier: Apache-2.0

//! Consuming, no-effect handoff after exact all-member confirmation. There is
//! no reverse conversion, raw-command projection, callback, or cleanup-on-Drop.
//! Physical cleanup and live retirement remain gated. Owner tests select the
//! closed remote/embedded retirement; the raw local prerequisite is test-only.

use super::*;
use crate::node_runtime::pg_store::payload_staging::{TerminalSlotWork, TerminalWorkObservation};
pub(in crate::node_runtime) use window::{
    DirectIntroductionFilePreparation, DirectStagingFileFailure,
};
pub(in crate::node_runtime) use window::{
    DirectStagingPreparation, DirectStagingSnapshot, DirectTerminalInstallation,
    DirectTerminalInstallationObservation, DirectTerminalInstallationProof,
    DirectTerminalPreparation, StreamAppendInstallation, StreamAppendInstallationObservation,
    StreamAppendInstallationProof, StreamAppendPreparation,
};
pub(in crate::node_runtime) use window::{
    RemoteStagingAcknowledgementAttempt, StagingAcknowledgementBatch,
};

#[path = "complete_terminal_reservation.rs"]
mod reservation;
#[path = "complete_terminal_retirement.rs"]
mod retirement;
#[path = "complete_staging_window.rs"]
mod window;
pub(in crate::node_runtime) use reservation::{
    TerminalReservationAuthority, TerminalReservationObservation,
};
pub(in crate::node_runtime) use retirement::{
    CompleteRetirementAttempt, CompleteRetirementSendError, TerminalSlotRetirementAuthority,
    TerminalSlotRetirementObservation,
};
pub(in crate::node_runtime) use window::RemoteDirectStagingFileWriter;
#[cfg(test)]
pub(in crate::node_runtime) use window::RemoteStreamStagingFileWriter;
pub(in crate::node_runtime) use window::{
    AllocatedStreamWindow, ConfirmedStagingWindow, RemoteStreamAllocationAttempt,
    StagingWindowHandoffFailure, StreamWindowAllocationFailure, StreamWindowAllocationPermit,
};
pub(in crate::node_runtime) use window::{
    DirectStagingFileWriter, RemoteDirectFileAttempt, RemoteStreamFileAttempt,
    StreamStagingFileWriter,
};

#[must_use = "retain terminal custody until the admitted terminal protocol completes"]
pub(in crate::node_runtime) struct CompleteTerminalHandoff<'route> {
    owner: CompleteFanoutSender<'route>,
    work: TerminalWorkObservation,
    slot_work: TerminalSlotWork,
    reservation: TerminalReservationObservation,
    retained_finalizer_reservation: TerminalReservationObservation,
    retirement: TerminalSlotRetirementObservation,
}

#[must_use = "failed handoff retains the original fanout holder and its uncertainty"]
pub(in crate::node_runtime) struct TerminalHandoffFailure<'route> {
    owner: Box<CompleteFanoutSender<'route>>,
    error: FanoutError,
}

impl<'route> CompleteFanoutSender<'route> {
    pub(in crate::node_runtime) fn into_terminal(
        self,
    ) -> Result<CompleteTerminalHandoff<'route>, TerminalHandoffFailure<'route>> {
        if let Err(error) = self.completion() {
            return Err(TerminalHandoffFailure {
                owner: Box::new(self),
                error,
            });
        }
        let (work, slot_work) = match terminal_work_for_handoff(&self.route, &self.sender) {
            Ok(work) => work,
            Err(error) => {
                return Err(TerminalHandoffFailure {
                    owner: Box::new(self),
                    error,
                })
            }
        };
        Ok(CompleteTerminalHandoff::from_confirmed_owner(
            self, work, slot_work,
        ))
    }
}

// Owner-subtree only: derive work from the retained complete command, never
// caller-supplied flags. The consuming caller validates completion before this.
fn terminal_work_for_handoff(
    route: &CompleteFanoutRoute<'_>,
    sender: &CompletePublicationSender,
) -> Result<(TerminalWorkObservation, TerminalSlotWork), FanoutError> {
    let (work, slot_work) = match &sender.source {
        PublicationSource::Inspected(evidence) => {
            let context = &evidence
                .pending
                .as_ref()
                .expect("all-member completion requires a retained command")
                .0;
            (context.terminal_work(), context.terminal_slot_work())
        }
        PublicationSource::Submitted { evidence, .. } => {
            (evidence.terminal_work(), evidence.terminal_slot_work())
        }
    };
    // Historical all-member evidence can retire a quiescent slot or retain
    // command/source custody for its independent reservation release. A
    // release alone never authorizes physical cleanup. Creation retirement
    // additionally requires exact retained resource/window custody.
    if route.is_historical() && !work.is_quiescent() && !work.has_reservation_release() {
        return Err(StoreError::RouteCapabilitySubjectMismatch {
            operation: "historical terminal handoff still requires non-quiescent work integration",
        }
        .into());
    }
    Ok((work, slot_work))
}

impl<'route> CompleteTerminalHandoff<'route> {
    // Private to this owner subtree. No fallible work after consuming the
    // confirmed owner, and no reset of its command/installation/apply history.
    fn from_confirmed_owner(
        owner: CompleteFanoutSender<'route>,
        work: TerminalWorkObservation,
        slot_work: TerminalSlotWork,
    ) -> Self {
        let reservation = match work.reservation {
            crate::node_runtime::pg_store::payload_staging::TerminalReservationWork::NoCommandProof
            | crate::node_runtime::pg_store::payload_staging::TerminalReservationWork::RetainForStream =>
                TerminalReservationObservation::NotRequired,
            _ => TerminalReservationObservation::NoAttemptObserved,
        };
        Self {
            owner,
            work,
            slot_work,
            reservation,
            retained_finalizer_reservation: if work.retained_finalization_proof {
                TerminalReservationObservation::NoAttemptObserved
            } else {
                TerminalReservationObservation::NotRequired
            },
            retirement: TerminalSlotRetirementObservation::NoAttemptObserved,
        }
    }
}

impl<'route> TerminalHandoffFailure<'route> {
    pub(in crate::node_runtime) fn error(&self) -> &FanoutError {
        &self.error
    }

    /// Return the SAME holder, not a fresh submission or a newly admitted cohort.
    pub(in crate::node_runtime) fn into_fanout(self) -> CompleteFanoutSender<'route> {
        *self.owner
    }
}

impl CompleteTerminalHandoff<'_> {
    pub(in crate::node_runtime) fn id(&self) -> MetadataCommandId {
        self.owner.id()
    }

    /// Historical observations only. Empty command work is not cleanup success:
    /// live/retained owners may have other obligations beyond this exact command.
    pub(in crate::node_runtime) fn work(&self) -> TerminalWorkObservation {
        self.work
    }

    pub(in crate::node_runtime) fn require_valid(&self) -> Result<(), StoreError> {
        self.owner.require_authority()
    }

    pub(in crate::node_runtime) fn application_observation(
        &self,
        node: NodeId,
    ) -> ApplicationObservation {
        self.owner.application_observation(node)
    }

    #[cfg(test)]
    pub(in crate::node_runtime::pg_store) fn installation_observation(
        &self,
    ) -> Option<ContextSubmissionObservation> {
        self.owner.installation_observation()
    }
}
