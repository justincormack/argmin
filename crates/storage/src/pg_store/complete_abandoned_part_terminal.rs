// Copyright The Argmin Authors.
// SPDX-License-Identifier: Apache-2.0

//! Consuming, no-effect handoff of the original source/cleanup pair after every
//! admitted member confirms cleanup application. No reconstruction as an ordinary
//! sender, source extraction, slot retirement or cleanup-on-Drop. The closed
//! finalizer release borrows this owner and retains its independent history.

use super::*;
use crate::node_runtime::pg_store::payload_staging::TerminalWorkObservation;
use crate::BucketSnapshotLoadError;

#[cfg(test)]
thread_local! {
    static INTERRUPT_BEFORE_RELEASE: std::cell::Cell<bool> = const { std::cell::Cell::new(false) };
}

#[must_use = "retain the source and cleanup until their admitted terminal protocol completes"]
pub(in crate::node_runtime) struct AbandonedPartTerminalHandoff<'route> {
    owner: PreparedAbandonedPart<'route>,
    work: TerminalWorkObservation,
    retained_finalizer_reservation: TerminalReservationObservation,
}

/// Borrowed only inside the terminal owner's closed release operation. Neither
/// the complete context nor the mutable observation can escape separately.
pub(in crate::node_runtime) struct AbandonedPartFinalizerRelease<'a> {
    route: &'a CompleteFanoutRoute<'a>,
    cleanup: &'a ContextualCommand,
    cutoff: CapturedPublicationDeadline,
    observation: &'a mut TerminalReservationObservation,
}

#[must_use = "failed handoff retains both commands and every previous observation"]
pub(in crate::node_runtime) struct AbandonedPartTerminalFailure<'route> {
    owner: Box<PreparedAbandonedPart<'route>>,
    error: FanoutError,
}

impl<'route> PreparedAbandonedPart<'route> {
    pub(in crate::node_runtime) fn into_terminal(
        self,
    ) -> Result<AbandonedPartTerminalHandoff<'route>, AbandonedPartTerminalFailure<'route>> {
        if let Err(error) = self.require_terminal_handoff() {
            return Err(AbandonedPartTerminalFailure {
                owner: Box::new(self),
                error,
            });
        }
        let work = self.candidate.cleanup.terminal_work();
        // Move the complete pair. In particular, do not derive a new sender
        // from the cleanup and discard the original finalizer/deadline/history.
        Ok(AbandonedPartTerminalHandoff {
            owner: self,
            work,
            retained_finalizer_reservation: TerminalReservationObservation::NoAttemptObserved,
        })
    }

    fn require_terminal_handoff(&self) -> Result<(), FanoutError> {
        self.require_valid()?;
        if self.installation != AbandonedPartInstallationObservation::Installed {
            return Err(StoreError::RouteCapabilitySubjectMismatch {
                operation: "abandoned part terminal handoff requires confirmed installation",
            }
            .into());
        }
        if self.publication != PublicationObservation::MarkerConfirmed {
            return Err(ApplicationAttemptError::PublicationNotConfirmed.into());
        }
        let route = &self.owner.owner.owner.route;
        let required = route.nodes().count();
        let confirmed = route
            .nodes()
            .filter(|node| self.cleanup_application(*node) == ApplicationObservation::Applied)
            .count();
        if confirmed != required {
            return Err(FanoutError::Incomplete {
                confirmed,
                required,
            });
        }
        Ok(())
    }
}

impl<'route> AbandonedPartTerminalFailure<'route> {
    pub(in crate::node_runtime) fn error(&self) -> &FanoutError {
        &self.error
    }

    pub(in crate::node_runtime) fn into_prepared(self) -> PreparedAbandonedPart<'route> {
        *self.owner
    }
}

impl AbandonedPartTerminalHandoff<'_> {
    /// Fresh local diagnostics only; never sufficient to authorize retirement.
    pub(in crate::node_runtime) fn inspect_retained_custody(
        &self,
        section: &EmbeddedPublicationSection<'_>,
    ) -> Result<
        crate::node_runtime::pg_store::AbandonedPartCustodyObservation,
        crate::node_runtime::pg_store::AbandonedPartCustodyError,
    > {
        self.require_valid()?;
        self.owner.inspect_cleanup_custody(section)
    }

    pub(in crate::node_runtime) fn retained_finalizer_reservation_observation(
        &self,
    ) -> TerminalReservationObservation {
        self.retained_finalizer_reservation
    }

    pub(in crate::node_runtime) fn release_retained_finalizer_reservation(
        &mut self,
    ) -> Result<(), BucketSnapshotLoadError> {
        self.require_valid()?;
        if self.retained_finalizer_reservation == TerminalReservationObservation::Released {
            return Ok(());
        }
        let fanout = &self.owner.owner.owner.owner;
        TerminalReservationAuthority::release_abandoned_part_finalizer(
            AbandonedPartFinalizerRelease {
                route: &fanout.route,
                cleanup: &self.owner.candidate.cleanup,
                cutoff: fanout.captured_deadline(),
                observation: &mut self.retained_finalizer_reservation,
            },
        )
    }

    #[cfg(test)]
    pub(in crate::node_runtime::pg_store) fn test_interrupt_retained_finalizer_release(
        &mut self,
    ) -> Result<(), BucketSnapshotLoadError> {
        struct Reset(bool);
        impl Drop for Reset {
            fn drop(&mut self) {
                INTERRUPT_BEFORE_RELEASE.with(|flag| flag.set(self.0));
            }
        }
        let _reset = Reset(INTERRUPT_BEFORE_RELEASE.with(|flag| flag.replace(true)));
        self.release_retained_finalizer_reservation()
    }

    pub(in crate::node_runtime) fn source_id(&self) -> MetadataCommandId {
        self.owner.source_id()
    }

    pub(in crate::node_runtime) fn cleanup_id(&self) -> MetadataCommandId {
        self.owner.cleanup_id()
    }

    /// Diagnostic obligations, never evidence of release, deletion or retirement.
    pub(in crate::node_runtime) fn work(&self) -> TerminalWorkObservation {
        self.work
    }

    pub(in crate::node_runtime) fn require_valid(&self) -> Result<(), StoreError> {
        self.owner.require_valid()
    }

    pub(in crate::node_runtime) fn cleanup_application(
        &self,
        node: NodeId,
    ) -> ApplicationObservation {
        self.owner.cleanup_application(node)
    }

    #[cfg(test)]
    pub(in crate::node_runtime::pg_store) fn assert_custody_for_test(
        &self,
        source: &ContextualCommand,
        cleanup: &ContextualCommand,
        topology: &crate::PgTopology,
        original: Option<ContextSubmissionObservation>,
    ) {
        self.owner
            .assert_contexts_for_test(source, cleanup, topology);
        assert_eq!(self.owner.installation_observation(), original);
        assert_eq!(
            self.owner.cleanup_installation(),
            AbandonedPartInstallationObservation::Installed
        );
        assert_eq!(
            self.owner.cleanup_publication(),
            PublicationObservation::MarkerConfirmed
        );
    }
}

impl<'a> AbandonedPartFinalizerRelease<'a> {
    pub(in crate::node_runtime::pg_store) fn route(&self) -> &'a CompleteFanoutRoute<'a> {
        self.route
    }

    pub(in crate::node_runtime::pg_store) fn cutoff(
        &self,
    ) -> (
        std::time::Instant,
        crate::storage_rpc::StorageRpcOperationDeadline,
    ) {
        (self.cutoff.local, self.cutoff.portable)
    }

    pub(in crate::node_runtime::pg_store) fn identity(
        &self,
    ) -> Result<(MetadataCommandId, [u8; 32]), StoreError> {
        let encoded = self.cleanup.encode_command().map_err(|_| {
            StoreError::RouteCapabilitySubjectMismatch {
                operation: "encode abandoned part terminal release context",
            }
        })?;
        Ok((encoded.id(), encoded.command_sha256()))
    }

    pub(in crate::node_runtime::pg_store) fn release(
        self,
        authority: TerminalReservationAuthority<'_>,
    ) -> Result<(), BucketSnapshotLoadError> {
        authority.require_context(self.cleanup)?;
        *self.observation = TerminalReservationObservation::Unconfirmed;
        #[cfg(test)]
        if INTERRUPT_BEFORE_RELEASE.with(|flag| flag.replace(false)) {
            panic!("interrupt abandoned part release after recording uncertainty");
        }
        self.cleanup.release_terminal_reservation(authority)?;
        *self.observation = TerminalReservationObservation::Released;
        Ok(())
    }
}
