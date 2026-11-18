// Copyright The Argmin Authors.
// SPDX-License-Identifier: Apache-2.0

//! Complete-context ownership and V14 local installation, preserving all prior
//! observations. This is NOT the live RPC/frontend authenticated-carrier switch.
//! No submission API sends only the v13 draft or exports its complete encoding:
//! doing so would detach indispensable context and installation history.

use super::super::command_context::{ContextError, ContextReissueFailure, ContextualCommand};
use super::{reissue::StagingReissue, InstallationObservation, StagingSubmission};
use crate::metadata_command::MetadataCommandId;

#[path = "payload_staging_remote_append_submission.rs"]
mod remote_append;
pub(in crate::node_runtime) use remote_append::RemoteStreamAppendInstallationAttempt;

#[path = "payload_staging_remote_direct_submission.rs"]
mod remote_direct;
pub(in crate::node_runtime) use remote_direct::RemoteDirectInstallationAttempt;

#[path = "payload_staging_remote_introduction_submission.rs"]
mod remote_introduction;
pub(in crate::node_runtime) use remote_introduction::RemoteDirectIntroductionAttempt;

#[path = "payload_staging_introduction_response.rs"]
mod introduction_response;
#[path = "payload_staging_submission_publication.rs"]
mod publication;
pub(in crate::node_runtime::pg_store) use publication::SubmittedPublicationEvidence;

#[path = "payload_staging_context_fresh_submission.rs"]
mod fresh;

#[cfg(test)]
pub(in super::super::super) use fresh::FreshContextSubmissionError;

#[path = "payload_staging_context_submission_preparation.rs"]
mod preparation;

#[path = "payload_staging_context_reissue_preparation.rs"]
mod reissue_preparation;

#[path = "payload_staging_context_replacement.rs"]
mod replacement;
pub(in super::super::super) use replacement::CompletePendingReplacement;

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
    pub(in super::super::super) fn complete_command_sha256(
        &self,
    ) -> Result<[u8; 32], super::super::command_context::CarrierError> {
        Ok(self.context.encode_command()?.command_sha256())
    }
    #[cfg(test)]
    pub(in super::super::super) fn assert_command_for_test(
        &self,
        expected: &crate::metadata_command::MetadataCommandEnvelope,
    ) {
        self.context.assert_command_for_test(expected);
    }
    /// Submit the complete carrier and derive all indexes in the actual slot
    /// transaction. No encoded command or independently installable projection
    /// escapes this holder. This is not yet the live RPC/frontend switch.
    pub(in super::super::super) fn install(
        &mut self,
        pg: &crate::node_runtime::pg_store::PgStore,
        node_id: u32,
        topology: &crate::PgTopology,
        effect_fence: crate::types::AdmittedRouteEffectFence,
        operation_deadline: Option<std::time::Instant>,
    ) -> Result<(), super::SubmissionError> {
        self.install_with_direct(
            pg,
            node_id,
            topology,
            effect_fence,
            operation_deadline,
            None,
        )
    }

    pub(in super::super::super) fn install_direct_terminal(
        &mut self,
        pg: &crate::node_runtime::pg_store::PgStore,
        node_id: u32,
        topology: &crate::PgTopology,
        fence: crate::types::AdmittedRouteEffectFence,
        deadline: std::time::Instant,
        proof: &crate::node_runtime::pg_store::DirectTerminalInstallationProof<'_, '_>,
    ) -> Result<(), super::SubmissionError> {
        self.install_with_direct(pg, node_id, topology, fence, Some(deadline), Some(proof))
    }

    fn install_with_direct(
        &mut self,
        pg: &crate::node_runtime::pg_store::PgStore,
        node_id: u32,
        topology: &crate::PgTopology,
        effect_fence: crate::types::AdmittedRouteEffectFence,
        operation_deadline: Option<std::time::Instant>,
        direct: Option<&crate::node_runtime::pg_store::DirectTerminalInstallationProof<'_, '_>>,
    ) -> Result<(), super::SubmissionError> {
        use super::SubmissionError;
        use crate::node_runtime::pg_store::PendingMetadataCommandSlotInsertError as Failure;
        if let Err(error) = pg.require_usable_connection() {
            self.installation = InstallationObservation::Unconfirmed;
            return Err(SubmissionError::Unconfirmed(error));
        }
        if !pg.conn.is_autocommit() {
            return Err(SubmissionError::OpenTransaction);
        }
        let encoded = self.context.encode_command()?;
        let previous = self.installation;
        // Includes panic/unwind, commit ambiguity and failed rollback. A later
        // definitive rejection may restore history, never reset it to fresh.
        self.installation = InstallationObservation::Unconfirmed;
        match pg.try_insert_complete_pending_with_direct_proof(
            node_id,
            &encoded,
            topology,
            effect_fence,
            operation_deadline,
            direct.map(super::super::DirectInstallationCheck::Embedded),
        ) {
            Ok(()) => {
                self.installation = InstallationObservation::Installed;
                Ok(())
            }
            Err(Failure::Definitive(error)) => {
                self.installation = previous;
                Err(SubmissionError::Rejected(error))
            }
            Err(Failure::MayHaveApplied(error)) => Err(SubmissionError::Unconfirmed(error)),
        }
    }

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
    /// One same-epoch replacement attempt through the shared SQL transaction.
    /// Both complete contexts remain owned here until committed replacement or
    /// exact successor verification is observed. This grants no recovery
    /// admission and does not extend the retained physical-write cutoff.
    pub(in super::super::super) fn replace(
        &mut self,
        pg: &crate::node_runtime::pg_store::PgStore,
        node_id: u32,
        topology: &crate::PgTopology,
    ) -> Result<bool, super::SubmissionError> {
        pg.replace_complete_pending_metadata_command_slot_for_reissue(
            node_id,
            CompletePendingReplacement::new(self, topology),
        )
    }

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
    pub(in super::super::super) fn assert_encoded_for_test(&self, expected: &[u8]) {
        assert_eq!(self.context.encode_field().unwrap(), expected);
    }

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
    /// Exercise the store entry independently of the convenience replace
    /// method. Production exposes no lender outside that method.
    pub(in super::super::super) fn replacement_attempt_for_test<'a>(
        &'a mut self,
        topology: &'a crate::PgTopology,
    ) -> CompletePendingReplacement<'a> {
        CompletePendingReplacement::new(self, topology)
    }

    /// Adversarial owner-local input only: production derives the successor
    /// from the retained source. Even malformed tests use the owning attempt
    /// capability and outcome recording, never a raw store-call adapter.
    pub(in super::super::super) fn from_contexts_for_test(
        source: ContextualCommand,
        successor: ContextualCommand,
    ) -> Self {
        Self {
            source: source.into_submission(),
            successor: successor.into_submission(),
        }
    }

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
