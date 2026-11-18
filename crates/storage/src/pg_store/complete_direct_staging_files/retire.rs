// Copyright The Argmin Authors.
// SPDX-License-Identifier: Apache-2.0

//! All-member object references replace frontend file custody. The exact command
//! proof may be released. Slot retirement checks durable custody transactionally;
//! including displaced-payload handoffs. It never retires the owner.
use super::*;

#[path = "remote_terminal.rs"]
mod remote;
pub(in crate::node_runtime) use remote::RemoteDirectPutTerminal;

#[must_use]
pub(in crate::node_runtime) struct DirectPutTerminal<'route> {
    terminal: CompleteTerminalHandoff<'route>,
}

#[must_use]
pub(in crate::node_runtime) struct DirectPutTerminalFailure<'route> {
    publication: Box<DirectTerminalPublication<'route>>,
    error: FanoutError,
}

impl<'route> DirectPutTerminalFailure<'route> {
    pub(in crate::node_runtime) fn error(&self) -> &FanoutError {
        &self.error
    }
    pub(in crate::node_runtime) fn into_publication(self) -> DirectTerminalPublication<'route> {
        *self.publication
    }
}

impl<'route> DirectTerminalPublication<'route> {
    pub(in crate::node_runtime) fn into_terminal(
        self,
    ) -> Result<DirectPutTerminal<'route>, DirectPutTerminalFailure<'route>> {
        self.into_terminal_parts(false)
            .map(|(terminal, _)| DirectPutTerminal { terminal })
    }

    pub(in crate::node_runtime) fn into_remote_terminal(
        self,
    ) -> Result<RemoteDirectPutTerminal<'route>, DirectPutTerminalFailure<'route>> {
        self.into_terminal_parts(true).map(|(terminal, identity)| {
            RemoteDirectPutTerminal::new(terminal, identity.expect("checked remote session"))
        })
    }

    fn into_terminal_parts(
        self,
        remote: bool,
    ) -> Result<
        (
            CompleteTerminalHandoff<'route>,
            Option<crate::node_client::MetadataCommandSessionIdentity>,
        ),
        DirectPutTerminalFailure<'route>,
    > {
        // No ownership split until every check succeeds. In particular, a lost
        // reply is not all-member completion, even when the replica committed.
        let checked = (|| {
            if self.custody.prepared.custody.preparation_session.is_some() != remote {
                return Err(Error::Window(
                    "remote direct terminal requires a session-bound handoff".into(),
                )
                .into());
            }
            let writer = self.custody.prepared.custody.writer();
            writer.window.require_valid()?;
            writer.fence.require_valid_for(self.id().cluster_epoch())?;
            let PublicationSource::Submitted { deadline, .. } = &self.sender.source else {
                unreachable!()
            };
            let cutoff = deadline
                .0
                .get()
                .expect("direct publication retains original cutoff");
            if Instant::now() >= cutoff.local
                || crate::clock::current_time_millis()
                    >= cutoff.portable.portable_wall_valid_until_ms
            {
                return Err(StoreError::OperationDeadlineExceeded {
                    context: "direct terminal original publication cutoff",
                }
                .into());
            }
            let route = &writer.window.terminal.owner.route;
            require_cohort_completion(route, &self.sender)?;
            terminal_work_for_handoff(route, &self.sender)
        })();
        let (work, slot_work) = match checked {
            Ok(work) => work,
            Err(error) => {
                return Err(DirectPutTerminalFailure {
                    publication: Box::new(self),
                    error,
                })
            }
        };
        let Self { custody, sender } = self;
        let identity = custody.prepared.custody.preparation_session;
        let writer = match custody.prepared.custody.custody {
            Custody::Embedded(writer) => writer,
            Custody::Remote(remote) => remote.writer,
        };
        // Every original member has committed the exact object references and
        // retained resource obligations. Discard only resolved frontend file
        // bookkeeping, retaining the same route, complete source and journals.
        let route = writer.window.terminal.owner.route;
        Ok((
            CompleteTerminalHandoff::from_confirmed_owner(
                CompleteFanoutSender { route, sender },
                work,
                slot_work,
            ),
            identity,
        ))
    }
}

impl DirectPutTerminal<'_> {
    pub(in crate::node_runtime) fn id(&self) -> MetadataCommandId {
        self.terminal.id()
    }
    pub(in crate::node_runtime) fn work(&self) -> TerminalWorkObservation {
        self.terminal.work()
    }
    pub(in crate::node_runtime) fn application(&self, node: NodeId) -> ApplicationObservation {
        self.terminal.application_observation(node)
    }
    pub(in crate::node_runtime) fn reservation(&self) -> TerminalReservationObservation {
        self.terminal.reservation_observation()
    }
    pub(in crate::node_runtime) fn release_reservation(
        &mut self,
    ) -> Result<(), crate::BucketSnapshotLoadError> {
        self.terminal.release_reservation()
    }
    pub(in crate::node_runtime) fn retirement(&self) -> TerminalSlotRetirementObservation {
        self.terminal.slot_retirement_observation()
    }
    pub(in crate::node_runtime) fn retire_embedded(
        &mut self,
        section: &EmbeddedPublicationSection<'_>,
    ) -> Result<(), crate::node_runtime::pg_store::CompleteRetirementSendError> {
        self.terminal
            .retire_embedded(section, self.terminal.owner.captured_deadline().local)
    }
    #[cfg(test)]
    pub(in crate::node_runtime::pg_store) fn test_installation(
        &self,
    ) -> Option<ContextSubmissionObservation> {
        self.terminal.installation_observation()
    }
    #[cfg(test)]
    pub(in crate::node_runtime::pg_store) fn test_interrupt_release(
        &mut self,
    ) -> Result<(), crate::BucketSnapshotLoadError> {
        self.terminal.test_interrupt_reservation_release()
    }
    #[cfg(test)]
    pub(in crate::node_runtime::pg_store) fn test_retire(
        &mut self,
        section: &EmbeddedPublicationSection<'_>,
        hook: impl FnMut(&'static str) -> Result<(), StoreError>,
    ) -> Result<(), crate::node_runtime::pg_store::CompleteRetirementSendError> {
        self.terminal.test_retire_embedded_with_hook(
            section,
            self.terminal.owner.captured_deadline().local,
            hook,
        )
    }
}
