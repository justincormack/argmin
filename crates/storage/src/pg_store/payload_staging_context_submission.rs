// Copyright The Argmin Authors.
// SPDX-License-Identifier: Apache-2.0

//! One-way promotion into complete-context ownership, preserving all prior
//! installation observations. This is NOT the authenticated-carrier switch.
//! There is intentionally no submission/encoding API for v13: it cannot carry
//! the context, so sending only its envelope would detach indispensable data.

use super::super::command_context::{ContextError, ContextReissueFailure, ContextualCommand};
use super::{reissue::StagingReissue, InstallationObservation, StagingSubmission};
use crate::metadata_command::MetadataCommandId;

#[path = "payload_staging_context_fresh_submission.rs"]
mod fresh;

#[cfg(test)]
pub(in super::super::super) use fresh::FreshContextSubmissionError;

#[path = "payload_staging_context_submission_preparation.rs"]
mod preparation;

#[path = "payload_staging_context_reissue_preparation.rs"]
mod reissue_preparation;

#[path = "payload_staging_context_epoch_reissue.rs"]
mod epoch_reissue;

#[path = "payload_staging_context_part_abandonment.rs"]
mod part_abandonment;

#[cfg(test)]
pub(in super::super::super) use part_abandonment::{
    ContextualPartAbandonment, ContextualPartAbandonmentReissue, PartAbandonmentPreparationFailure,
    PartAbandonmentReissueFailure, PartAbandonmentReissueInspectionFailure,
    PreparedPartAbandonmentReissue,
};

#[cfg(test)]
pub(in super::super::super) use epoch_reissue::{
    ContextualEpochReissue, EpochReissueInspectionFailure, PreparedEpochReissue,
};

#[cfg(test)]
pub(in super::super::super) use preparation::{
    ContextualSubmissionPreparationFailure, PreparedContextualSubmission,
};

#[cfg(test)]
pub(in super::super::super) use reissue_preparation::{
    ContextualReissueInspectionFailure, PreparedContextualReissue,
};

/// Copyable diagnostics only. Neither observation establishes current custody,
/// global absence, recovery admission, or permission to release resources.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(in super::super::super) struct ContextSubmissionObservation {
    pub(in super::super::super) command_id: MetadataCommandId,
    pub(in super::super::super) installation: InstallationObservation,
}

/// No Clone, raw context/envelope view, encoding, reset, split, or cleanup Drop.
/// Private fields live below the submission module: surrounding context code
/// cannot detach this value and reconstruct a fresh pre-submission description.
pub(in super::super::super) struct ContextualSubmission {
    context: ContextualCommand,
    installation: InstallationObservation,
}

pub(in super::super::super) struct ContextualReissue {
    source: ContextualSubmission,
    successor: ContextualSubmission,
}

/// Failed preparation owns the original complete context AND its installation
/// history. Returning the descriptive failure alone would erase that history.
pub(in super::super::super) struct ContextualReissuePreparationFailure {
    failure: ContextReissueFailure,
    installation: InstallationObservation,
}

impl std::fmt::Debug for ContextualReissuePreparationFailure {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("ContextualReissuePreparationFailure")
            .field("error", self.failure.error())
            .field("installation", &self.installation)
            .finish_non_exhaustive()
    }
}

impl ContextualReissuePreparationFailure {
    pub(in super::super::super) fn error(&self) -> &ContextError {
        self.failure.error()
    }

    pub(in super::super::super) fn into_submission(self) -> ContextualSubmission {
        ContextualSubmission {
            context: self.failure.into_context(),
            installation: self.installation,
        }
    }
}

impl ContextualCommand {
    /// Fresh descriptive input only. An unresolved holder never lends an input
    /// to this constructor; its consuming conversion below carries its history.
    pub(in super::super::super) fn into_submission(self) -> ContextualSubmission {
        ContextualSubmission {
            context: self,
            installation: InstallationObservation::NoAcceptanceObserved,
        }
    }
}

impl StagingSubmission {
    /// Transfer the exact description AND history without reinspection,
    /// serialization, recapture, or caller-selected topology/observation.
    pub(in super::super::super) fn into_contextual(self) -> ContextualSubmission {
        ContextualSubmission {
            context: ContextualCommand::from_described(self.description),
            installation: self.observation,
        }
    }
}

impl StagingReissue {
    /// Preserve both possible commands, including independent source and
    /// successor observations. No unresolved pair may select only its successor.
    pub(in super::super::super) fn into_contextual(self) -> ContextualReissue {
        ContextualReissue {
            source: self.source.into_contextual(),
            successor: ContextualSubmission {
                context: ContextualCommand::from_described(self.successor),
                installation: self.observation,
            },
        }
    }
}

impl ContextualSubmission {
    /// Prepare, never install or authorize, an ordinary same-epoch replacement.
    /// Keep the exact original context/history and derive the successor solely
    /// from it. No command fields, topology, clocks or replacement context are
    /// caller-supplied. The successor has not been sent by this holder.
    pub(in super::super::super) fn into_same_epoch_reissue(
        self,
        id: MetadataCommandId,
    ) -> Result<ContextualReissue, ContextualReissuePreparationFailure> {
        match self.context.into_same_epoch_reissue_pair(id) {
            Ok((source, successor)) => Ok(ContextualReissue {
                source: Self {
                    context: source,
                    installation: self.installation,
                },
                successor: Self {
                    context: successor,
                    installation: InstallationObservation::NoAcceptanceObserved,
                },
            }),
            Err(failure) => Err(ContextualReissuePreparationFailure {
                failure,
                installation: self.installation,
            }),
        }
    }

    pub(in super::super::super) fn observation(&self) -> ContextSubmissionObservation {
        ContextSubmissionObservation {
            command_id: self.context.id(),
            installation: self.installation,
        }
    }
}

impl ContextualReissue {
    pub(in super::super::super) fn source(&self) -> ContextSubmissionObservation {
        self.source.observation()
    }

    pub(in super::super::super) fn successor(&self) -> ContextSubmissionObservation {
        self.successor.observation()
    }

    /// Preserve the existing observed-replacement rule. This retains local
    /// history only; success is not a current slot/ref/replication certificate.
    pub(in super::super::super) fn into_observed_successor(
        self,
    ) -> Result<ContextualSubmission, Box<Self>> {
        if self.successor.installation != InstallationObservation::Installed {
            return Err(Box::new(self));
        }
        Ok(self.successor)
    }
}

// Owner-local full-identity assertions, not production extraction of a context
// or descriptor bytes that could be used to reset installation history.
#[cfg(test)]
impl ContextualSubmission {
    pub(in super::super::super) fn assert_context_for_test(
        &self,
        expected: &ContextualCommand,
        topology: &crate::PgTopology,
    ) {
        use super::super::command_context::PendingContextMatch;
        assert_eq!(
            self.context.pending_match(expected, topology).unwrap(),
            PendingContextMatch::ExactRetry,
        );
        assert_eq!(
            self.context.encode_field().unwrap(),
            expected.encode_field().unwrap()
        );
    }
}

#[cfg(test)]
impl ContextualReissue {
    pub(in super::super::super) fn assert_contexts_for_test(
        &self,
        source: &ContextualCommand,
        successor: &ContextualCommand,
        topology: &crate::PgTopology,
    ) {
        self.source.assert_context_for_test(source, topology);
        self.successor.assert_context_for_test(successor, topology);
    }
}
