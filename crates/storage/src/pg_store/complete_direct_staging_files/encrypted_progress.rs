// Copyright The Argmin Authors.
// SPDX-License-Identifier: Apache-2.0

//! One current-command effect per call. No internal retry, contender drain,
//! fresh authority, or successful completion from an unconfirmed observation.
use super::*;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(in crate::node_runtime) enum DirectPublicationProgress {
    MarkerConfirmed,
    ReplicaApplied(NodeId),
    /// An observation, not a terminal capability. The consuming handoff still
    /// revalidates original authority and all-member confirmation.
    ReadyForTerminal,
}

impl DirectPublicationProgress {
    pub(in crate::node_runtime) fn is_ready(self) -> bool {
        self == Self::ReadyForTerminal
    }
    pub(in crate::node_runtime) fn applied_node(self) -> Option<NodeId> {
        match self {
            Self::ReplicaApplied(node) => Some(node),
            _ => None,
        }
    }
}

impl BudgetedDirectOwner<DirectTerminalPublication<'_>> {
    pub(in crate::node_runtime) fn advance_embedded(
        &mut self,
        section: &EmbeddedPublicationSection<'_>,
    ) -> Result<DirectPublicationProgress, FanoutError> {
        self.budget.require_deadline()?;
        self.owner.require_embedded_progress(section)?;
        if self.publication() != PublicationObservation::MarkerConfirmed {
            self.publish_embedded(section)?;
            return Ok(DirectPublicationProgress::MarkerConfirmed);
        }
        if let Some(node) = self.owner.next_application() {
            self.apply_embedded(section, node)?;
            return Ok(DirectPublicationProgress::ReplicaApplied(node));
        }
        Ok(DirectPublicationProgress::ReadyForTerminal)
    }

    /// The caller supplies a connection only, never a node-selection authority.
    /// None selects the admitted embedded transport for the required replica.
    /// The original primary session remains held by every actual effect.
    pub(in crate::node_runtime) fn advance_remote(
        &mut self,
        primary: &UnixStorageNodeClient,
        session: &UnixStorageNodeMetadataCommandSession,
        target: Option<&UnixStorageNodeClient>,
    ) -> Result<DirectPublicationProgress, FanoutError> {
        self.budget.require_deadline()?;
        // Check even a fully confirmed owner before reporting Ready. This
        // temporary local borrow is released before the closed effect reborrows.
        self.owner.require_remote_progress(primary, session)?;
        if self.publication() != PublicationObservation::MarkerConfirmed {
            if target.is_some() {
                return Err(unexpected_target());
            }
            self.publish_remote(primary, session)?;
            return Ok(DirectPublicationProgress::MarkerConfirmed);
        }
        let Some(node) = self.owner.next_application() else {
            if target.is_some() {
                return Err(unexpected_target());
            }
            return Ok(DirectPublicationProgress::ReadyForTerminal);
        };
        if let Some(target) = target {
            if target.node_id() != node {
                return Err(unexpected_target());
            }
            self.apply_remote(primary, session, target)?;
        } else {
            self.apply_embedded_replica(primary, session, node)?;
        }
        Ok(DirectPublicationProgress::ReplicaApplied(node))
    }
}

fn unexpected_target() -> FanoutError {
    StoreError::RouteCapabilitySubjectMismatch {
        operation: "direct publication progress requires the next captured cohort target",
    }
    .into()
}
