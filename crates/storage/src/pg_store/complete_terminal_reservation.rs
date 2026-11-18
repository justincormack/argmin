// Copyright The Argmin Authors.
// SPDX-License-Identifier: Apache-2.0

//! Independent closed command/source-proof releases. Any entered attempt is conservatively
//! unconfirmed until acknowledgement; neither errors nor unwind reset custody.

use super::*;
use crate::node_runtime::pg_store::payload_staging::ContextualCommand;
use crate::BucketSnapshotLoadError;

#[cfg(test)]
thread_local! {
    static INTERRUPT_BEFORE_RELEASE: std::cell::Cell<bool> = const { std::cell::Cell::new(false) };
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(in crate::node_runtime) enum TerminalReservationObservation {
    /// Only the selected command/source proof, not all retained owner resources.
    NotRequired,
    NoAttemptObserved,
    Unconfirmed,
    Released,
}

#[derive(Clone, Copy)]
enum ReservationKind {
    Command,
    RetainedFinalizer,
}

/// Constructed only on the stack of the owning terminal operation. The private
/// representation binds the exact complete identity, admitted map and first
/// local/wire cutoff; it cannot be renewed or reconstructed by a caller.
pub(in crate::node_runtime) struct TerminalReservationAuthority<'a> {
    route: &'a CompleteFanoutRoute<'a>,
    id: MetadataCommandId,
    sha256: [u8; 32],
    cutoff: CapturedPublicationDeadline,
    kind: ReservationKind,
}

impl CompleteTerminalHandoff<'_> {
    pub(in crate::node_runtime) fn reservation_observation(
        &self,
    ) -> TerminalReservationObservation {
        self.reservation
    }

    pub(in crate::node_runtime) fn release_reservation(
        &mut self,
    ) -> Result<(), BucketSnapshotLoadError> {
        self.release_reservation_kind(ReservationKind::Command)
    }

    pub(in crate::node_runtime) fn retained_finalizer_reservation_observation(
        &self,
    ) -> TerminalReservationObservation {
        self.retained_finalizer_reservation
    }

    pub(in crate::node_runtime) fn release_retained_finalizer_reservation(
        &mut self,
    ) -> Result<(), BucketSnapshotLoadError> {
        self.release_reservation_kind(ReservationKind::RetainedFinalizer)
    }

    fn release_reservation_kind(
        &mut self,
        kind: ReservationKind,
    ) -> Result<(), BucketSnapshotLoadError> {
        self.require_valid()?;
        let observation = match kind {
            ReservationKind::Command => &mut self.reservation,
            ReservationKind::RetainedFinalizer => &mut self.retained_finalizer_reservation,
        };
        if matches!(
            *observation,
            TerminalReservationObservation::NotRequired | TerminalReservationObservation::Released
        ) {
            return Ok(());
        }
        let request = self
            .owner
            .sender
            .source
            .request(self.owner.route.primary())
            .map_err(|_| StoreError::RouteCapabilitySubjectMismatch {
                operation: "terminal release requires retained complete command",
            })?;
        let cutoff = match &self.owner.sender.source {
            PublicationSource::Inspected(evidence) => &evidence.publication_deadline,
            PublicationSource::Submitted { deadline, .. } => deadline,
        }
        .0
        .get()
        .copied()
        .expect("terminal holder retains first publication cutoff");
        let authority = TerminalReservationAuthority {
            route: &self.owner.route,
            id: request.id(),
            sha256: request.command_sha256(),
            cutoff,
            kind,
        };
        authority.require_valid()?;
        *observation = TerminalReservationObservation::Unconfirmed;
        #[cfg(test)]
        if INTERRUPT_BEFORE_RELEASE.with(|flag| flag.replace(false)) {
            panic!("interrupt terminal release after recording uncertainty");
        }
        match &self.owner.sender.source {
            PublicationSource::Inspected(evidence) => evidence
                .pending
                .as_ref()
                .expect("terminal command")
                .0
                .release_terminal_reservation(authority)?,
            PublicationSource::Submitted { evidence, .. } => {
                evidence.release_terminal_reservation(authority)?
            }
        }
        *observation = TerminalReservationObservation::Released;
        Ok(())
    }

    #[cfg(test)]
    pub(in crate::node_runtime::pg_store) fn test_require_release_context(
        &self,
        context: &ContextualCommand,
    ) -> Result<(), StoreError> {
        let request = self
            .owner
            .sender
            .source
            .request(self.owner.route.primary())
            .unwrap();
        let cutoff = match &self.owner.sender.source {
            PublicationSource::Inspected(evidence) => &evidence.publication_deadline,
            PublicationSource::Submitted { deadline, .. } => deadline,
        }
        .0
        .get()
        .copied()
        .unwrap();
        TerminalReservationAuthority {
            route: &self.owner.route,
            id: request.id(),
            sha256: request.command_sha256(),
            cutoff,
            kind: ReservationKind::Command,
        }
        .require_context(context)
    }

    #[cfg(test)]
    pub(in crate::node_runtime::pg_store) fn test_interrupt_reservation_release(
        &mut self,
    ) -> Result<(), BucketSnapshotLoadError> {
        self.test_interrupt_release(ReservationKind::Command)
    }

    #[cfg(test)]
    pub(in crate::node_runtime::pg_store) fn test_interrupt_retained_finalizer_release(
        &mut self,
    ) -> Result<(), BucketSnapshotLoadError> {
        self.test_interrupt_release(ReservationKind::RetainedFinalizer)
    }

    #[cfg(test)]
    fn test_interrupt_release(
        &mut self,
        kind: ReservationKind,
    ) -> Result<(), BucketSnapshotLoadError> {
        struct Reset(bool);
        impl Drop for Reset {
            fn drop(&mut self) {
                INTERRUPT_BEFORE_RELEASE.with(|flag| flag.set(self.0));
            }
        }
        let _reset = Reset(INTERRUPT_BEFORE_RELEASE.with(|flag| flag.replace(true)));
        self.release_reservation_kind(kind)
    }
}

impl TerminalReservationAuthority<'_> {
    /// No general constructor: only the terminal pair can lend this permit,
    /// which performs the send while retaining its observation on error/unwind.
    pub(in crate::node_runtime::pg_store) fn release_abandoned_part_finalizer(
        permit: AbandonedPartFinalizerRelease<'_>,
    ) -> Result<(), BucketSnapshotLoadError> {
        let (id, sha256) = permit.identity()?;
        let (local, portable) = permit.cutoff();
        let authority = TerminalReservationAuthority {
            route: permit.route(),
            id,
            sha256,
            cutoff: CapturedPublicationDeadline { local, portable },
            kind: ReservationKind::RetainedFinalizer,
        };
        authority.require_valid()?;
        permit.release(authority)
    }

    pub(in crate::node_runtime::pg_store) fn is_retained_finalizer(&self) -> bool {
        matches!(self.kind, ReservationKind::RetainedFinalizer)
    }

    pub(in crate::node_runtime::pg_store) fn require_context(
        &self,
        context: &ContextualCommand,
    ) -> Result<(), StoreError> {
        self.require_valid()?;
        let encoded =
            context
                .encode_command()
                .map_err(|_| StoreError::RouteCapabilitySubjectMismatch {
                    operation: "encode terminal reservation context",
                })?;
        if context.id() != self.id || encoded.command_sha256() != self.sha256 {
            return Err(StoreError::RouteCapabilitySubjectMismatch {
                operation: "terminal reservation complete context",
            });
        }
        Ok(())
    }

    pub(in crate::node_runtime::pg_store) fn route(&self) -> &CompleteFanoutRoute<'_> {
        self.route
    }

    pub(in crate::node_runtime::pg_store) fn require_valid(&self) -> Result<(), StoreError> {
        self.route.require_valid()?;
        if Instant::now() >= self.cutoff.local
            || crate::clock::current_time_millis()
                >= self.cutoff.portable.portable_wall_valid_until_ms
        {
            return Err(StoreError::OperationDeadlineExceeded {
                context: "terminal reservation original publication cutoff",
            });
        }
        Ok(())
    }

    pub(in crate::node_runtime::pg_store) fn cutoff(
        &self,
    ) -> (Instant, crate::storage_rpc::StorageRpcOperationDeadline) {
        (self.cutoff.local, self.cutoff.portable)
    }
}
