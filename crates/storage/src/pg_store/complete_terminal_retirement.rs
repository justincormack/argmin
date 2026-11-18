// Copyright The Argmin Authors.
// SPDX-License-Identifier: Apache-2.0

//! Narrow slot retirement. The direct local prerequisite remains test-only;
//! candidate closed remote/embedded operations select admitted mutation. Neither a
//! pending observation nor a scalar work report can mint this authority;
//! all-member terminal custody does.

use super::*;
#[cfg(test)]
use crate::node_runtime::pg_store::PgStore;
use crate::node_runtime::pg_store::{CompletePublicationError, CompletePublicationRequest};
#[cfg(test)]
use crate::node_runtime::server::CompletePublicationAuthority;

#[path = "complete_retirement_sender.rs"]
mod sender;
pub(in crate::node_runtime) use sender::{CompleteRetirementAttempt, CompleteRetirementSendError};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(in crate::node_runtime) enum TerminalSlotRetirementObservation {
    NoAttemptObserved,
    Unconfirmed,
    /// Only this exact pending slot, not the retained owner or its obligations.
    Retired,
}

/// Stack-only permit borrowed from the unchanged terminal holder. It is not a
/// replacement for storage-node admission: the transaction requires both.
pub(in crate::node_runtime) struct TerminalSlotRetirementAuthority<'a> {
    route: &'a CompleteFanoutRoute<'a>,
    id: MetadataCommandId,
    sha256: [u8; 32],
    cutoff: CapturedPublicationDeadline,
}

impl TerminalSlotRetirementAuthority<'_> {
    pub(in crate::node_runtime) fn require_embedded_section(
        &self,
        section: &crate::cluster::EmbeddedPublicationSection<'_>,
    ) -> Result<(), StoreError> {
        self.route.require_embedded_section(section)
    }

    pub(in crate::node_runtime::pg_store) fn require_request(
        &self,
        request: &CompletePublicationRequest<'_>,
    ) -> Result<(), StoreError> {
        self.route.require_valid()?;
        if request.id() != self.id
            || request.command_sha256() != self.sha256
            || request.node_id() != self.route.primary()
            || request.topology() != self.route.topology()
        {
            return Err(StoreError::RouteCapabilitySubjectMismatch {
                operation: "terminal slot retirement complete context/route",
            });
        }
        if Instant::now() >= self.cutoff.local
            || crate::clock::current_time_millis()
                >= self.cutoff.portable.portable_wall_valid_until_ms
        {
            return Err(StoreError::OperationDeadlineExceeded {
                context: "terminal slot retirement original cutoff",
            });
        }
        Ok(())
    }
}

impl CompleteTerminalHandoff<'_> {
    fn require_retirement_work(&self) -> Result<(), StoreError> {
        if !matches!(self.slot_work, TerminalSlotWork::Quiescent)
            && !(matches!(
                self.slot_work,
                TerminalSlotWork::UploadPartCreation | TerminalSlotWork::DirectPutPublication
            ) && self.reservation == TerminalReservationObservation::Released)
        {
            return Err(StoreError::RouteCapabilitySubjectMismatch {
                operation: "terminal slot retirement still requires terminal work integration",
            });
        }
        Ok(())
    }

    pub(in crate::node_runtime) fn slot_retirement_observation(
        &self,
    ) -> TerminalSlotRetirementObservation {
        self.retirement
    }

    // Owner-test transaction seam only. Production operations select the store
    // through closed remote or embedded admission, never a caller's callback.
    #[cfg(test)]
    fn retire_quiescent_pending(
        &mut self,
        pg: &PgStore,
        authority: &CompletePublicationAuthority<'_>,
        #[cfg(test)] hook: impl FnMut(&'static str) -> Result<(), StoreError>,
    ) -> Result<(), CompletePublicationError> {
        self.require_valid()?;
        self.require_retirement_work()?;
        if self.retirement == TerminalSlotRetirementObservation::Retired {
            // This is historical confirmation of our exact operation, not a
            // license to remove a later slot or to reset history on a retry.
            return Ok(());
        }
        let request = self
            .owner
            .sender
            .source
            .request(self.owner.route.primary())
            .map_err(|_| StoreError::RouteCapabilitySubjectMismatch {
                operation: "terminal slot retirement requires retained command",
            })?;
        let cutoff = match &self.owner.sender.source {
            PublicationSource::Inspected(evidence) => &evidence.publication_deadline,
            PublicationSource::Submitted { deadline, .. } => deadline,
        }
        .0
        .get()
        .copied()
        .expect("terminal retains first publication cutoff");
        let terminal = TerminalSlotRetirementAuthority {
            route: &self.owner.route,
            id: request.id(),
            sha256: request.command_sha256(),
            cutoff,
        };
        terminal.require_request(&request)?;
        // Never reset an earlier ambiguous attempt, including on unwind or
        // failure after DELETE/COMMIT. Drop performs no retry or other effect.
        self.retirement = TerminalSlotRetirementObservation::Unconfirmed;
        request.retire_quiescent_pending(
            pg,
            authority,
            &terminal,
            #[cfg(test)]
            hook,
        )?;
        self.retirement = TerminalSlotRetirementObservation::Retired;
        Ok(())
    }

    #[cfg(test)]
    pub(in crate::node_runtime::pg_store) fn test_retire_quiescent_pending(
        &mut self,
        pg: &PgStore,
        authority: &CompletePublicationAuthority<'_>,
        hook: impl FnMut(&'static str) -> Result<(), StoreError>,
    ) -> Result<(), CompletePublicationError> {
        self.retire_quiescent_pending(pg, authority, hook)
    }
}
