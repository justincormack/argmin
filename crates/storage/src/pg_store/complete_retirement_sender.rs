// Copyright The Argmin Authors.
// SPDX-License-Identifier: Apache-2.0

//! Closed remote/embedded attempt owned by the all-member terminal holder.
//! Admission is selected only by owner tests; live pending removal still rejects
//! this grammar. No detached request, resettable history or cleanup-on-Drop API.

use super::*;
use crate::node_runtime::clients::{UnixStorageNodeClient, UnixStorageNodeMetadataCommandSession};

impl CompletePublicationSender {
    pub(in crate::node_runtime::pg_store) fn retire_remote_direct_introduction(
        scope: crate::node_runtime::pg_store::payload_staging::RemoteIntroductionRetirementScope<
            '_,
            '_,
        >,
        held: &mut crate::node_client::CompletePrimarySessionBorrow<'_>,
    ) -> Result<(), crate::node_runtime::pg_store::StagingWindowConfirmationError> {
        use crate::node_runtime::pg_store::StagingWindowConfirmationError as Error;
        scope.require_session(held)?;
        let (sender, route, deadline, observation) = scope.into_parts();
        let validate = || {
            sender.require_cohort_subject(route)?;
            require_cohort_completion(route, sender)?;
            let (_, work) = terminal_work_for_handoff(route, sender)?;
            if !matches!(work, TerminalSlotWork::Quiescent) {
                return Err(StoreError::RouteCapabilitySubjectMismatch {
                    operation: "remote introduction retirement requires quiescent slot work",
                }
                .into());
            }
            Ok::<_, FanoutError>(())
        };
        validate().map_err(|e| Error::Window(e.to_string()))?;
        if *observation == TerminalSlotRetirementObservation::Retired {
            return Ok(());
        }
        let request = sender
            .source
            .request(route.primary())
            .map_err(|e| Error::Window(e.to_string()))?;
        let cutoff = match &sender.source {
            PublicationSource::Inspected(evidence) => &evidence.publication_deadline,
            PublicationSource::Submitted { deadline, .. } => deadline,
        }
        .0
        .get()
        .copied()
        .expect("confirmed introduction retains original cutoff");
        let terminal = TerminalSlotRetirementAuthority {
            route,
            id: request.id(),
            sha256: request.command_sha256(),
            cutoff,
        };
        terminal.require_request(&request)?;
        let transport = request.prepare_transport(deadline);
        let previous = *observation;
        *observation = TerminalSlotRetirementObservation::Unconfirmed;
        held.send_complete_retirement(CompleteRetirementAttempt {
            transport: &transport,
            terminal: &terminal,
            observation,
            previous,
        })
        .map_err(|e| Error::Window(e.to_string()))
    }

    /// The opaque scope can only be borrowed from the owning introduction
    /// terminal. No independent sender/cohort/history entry point is exposed.
    pub(in crate::node_runtime::pg_store) fn retire_direct_introduction(
        scope: crate::node_runtime::pg_store::payload_staging::DirectIntroductionRetirementScope<
            '_,
            '_,
            '_,
        >,
        #[cfg(test)] hook: impl FnMut(&'static str) -> Result<(), StoreError>,
    ) -> Result<(), crate::node_runtime::pg_store::StagingWindowConfirmationError> {
        use crate::node_runtime::pg_store::StagingWindowConfirmationError as Error;
        let (sender, route, section, deadline, observation) = scope.into_parts();
        let validate = || {
            sender.require_cohort_subject(route)?;
            route.require_embedded_section(section)?;
            require_cohort_completion(route, sender)?;
            let (_, work) = terminal_work_for_handoff(route, sender)?;
            if !matches!(work, TerminalSlotWork::Quiescent) {
                return Err(StoreError::RouteCapabilitySubjectMismatch {
                    operation: "introduction retirement requires quiescent slot work",
                }
                .into());
            }
            Ok::<_, FanoutError>(())
        };
        validate().map_err(|error| Error::Window(error.to_string()))?;
        // Retired is historical confirmation. Never downgrade it on a later
        // failed retry or attempt to remove another pending slot.
        if *observation == TerminalSlotRetirementObservation::Retired {
            return Ok(());
        }
        let request =
            sender
                .source
                .request(route.primary())
                .map_err(|error| StoreError::StorageRpc {
                    node_id: route.primary().as_u32(),
                    operation: "prepare introduction retirement",
                    failure: crate::storage_rpc::StorageRpcErrorCode::PayloadDecode,
                    detail: crate::StorageNodeFailureDetail::new(error.to_string()),
                })?;
        let cutoff = match &sender.source {
            PublicationSource::Inspected(evidence) => &evidence.publication_deadline,
            PublicationSource::Submitted { deadline, .. } => deadline,
        }
        .0
        .get()
        .copied()
        .expect("confirmed introduction retains original cutoff");
        let terminal = TerminalSlotRetirementAuthority {
            route,
            id: request.id(),
            sha256: request.command_sha256(),
            cutoff,
        };
        terminal.require_request(&request)?;
        let transport = request.prepare_transport(deadline);
        *observation = TerminalSlotRetirementObservation::Unconfirmed;
        transport
            .retire_embedded(
                section,
                &terminal,
                #[cfg(test)]
                hook,
            )
            .map_err(|error| Error::Window(error.to_string()))?;
        *observation = TerminalSlotRetirementObservation::Retired;
        Ok(())
    }
}

#[derive(Debug, thiserror::Error)]
pub(in crate::node_runtime) enum CompleteRetirementSendError {
    #[error("embedded complete retirement unconfirmed: {0}")]
    Embedded(#[from] CompletePublicationError),
    #[error("complete retirement not sent: {0}")]
    NotSent(StoreError),
    #[error("complete retirement unconfirmed: {0}")]
    Unconfirmed(StoreError),
}

pub(in crate::node_runtime) struct CompleteRetirementAttempt<'a> {
    transport: &'a CompletePublicationTransport<'a>,
    terminal: &'a TerminalSlotRetirementAuthority<'a>,
    observation: &'a mut TerminalSlotRetirementObservation,
    previous: TerminalSlotRetirementObservation,
}

impl CompleteRetirementAttempt<'_> {
    pub(in crate::node_runtime) fn node_id(&self) -> NodeId {
        self.transport.node_id()
    }
    pub(in crate::node_runtime) fn id(&self) -> MetadataCommandId {
        self.transport.id()
    }
    pub(in crate::node_runtime) fn deadline(&self) -> Instant {
        self.transport.deadline()
    }

    pub(in crate::node_runtime) fn encode_request(&self) -> Result<Vec<u8>, StoreError> {
        self.transport
            .encode_retirement_request(self.terminal)
            .map_err(|error| match error {
                crate::node_runtime::pg_store::CompletePendingInspectionError::Store(error) => {
                    error
                }
                other => self.payload_error("encode complete retirement", other.to_string()),
            })
    }

    pub(in crate::node_runtime) fn not_sent(
        self,
        error: StoreError,
    ) -> CompleteRetirementSendError {
        *self.observation = self.previous;
        CompleteRetirementSendError::NotSent(error)
    }

    pub(in crate::node_runtime) fn unconfirmed(
        self,
        error: StoreError,
    ) -> CompleteRetirementSendError {
        CompleteRetirementSendError::Unconfirmed(error)
    }

    pub(in crate::node_runtime) fn confirm(
        self,
        bytes: &[u8],
    ) -> Result<(), CompleteRetirementSendError> {
        self.transport
            .decode_retirement_confirmation(bytes)
            .map_err(|error| {
                CompleteRetirementSendError::Unconfirmed(
                    self.payload_error("confirm complete retirement", error.to_string()),
                )
            })?;
        // Exact late responses may confirm past work. They do not renew either
        // captured cutoff or authorize another operation.
        *self.observation = TerminalSlotRetirementObservation::Retired;
        Ok(())
    }

    fn payload_error(&self, operation: &'static str, detail: String) -> StoreError {
        StoreError::StorageRpc {
            node_id: self.node_id().as_u32(),
            operation,
            failure: crate::storage_rpc::StorageRpcErrorCode::PayloadDecode,
            detail: crate::StorageNodeFailureDetail::new(detail),
        }
    }
}

impl CompleteTerminalHandoff<'_> {
    /// Retry can reopen the original historical primary session, but cannot
    /// renew custody's frozen work/route cutoff or reset retirement uncertainty.
    pub(in crate::node_runtime) fn open_primary_session(
        &self,
        client: &UnixStorageNodeClient,
        deadline: Instant,
    ) -> Result<UnixStorageNodeMetadataCommandSession, StoreError> {
        self.owner.open_primary_session(client, deadline)
    }

    pub(in crate::node_runtime) fn retire_embedded(
        &mut self,
        section: &crate::cluster::EmbeddedPublicationSection<'_>,
        deadline: Instant,
    ) -> Result<(), CompleteRetirementSendError> {
        self.retire_embedded_inner(
            section,
            deadline,
            #[cfg(test)]
            |_| Ok(()),
        )
    }

    fn retire_embedded_inner(
        &mut self,
        section: &crate::cluster::EmbeddedPublicationSection<'_>,
        deadline: Instant,
        #[cfg(test)] hook: impl FnMut(&'static str) -> Result<(), StoreError>,
    ) -> Result<(), CompleteRetirementSendError> {
        self.owner
            .route
            .require_embedded_section(section)
            .map_err(CompleteRetirementSendError::NotSent)?;
        self.with_retirement_attempt(deadline, |attempt| {
            attempt.transport.retire_embedded(
                section,
                attempt.terminal,
                #[cfg(test)]
                hook,
            )?;
            *attempt.observation = TerminalSlotRetirementObservation::Retired;
            Ok(())
        })
    }

    #[cfg(test)]
    pub(in crate::node_runtime::pg_store) fn test_retire_embedded_with_hook(
        &mut self,
        section: &crate::cluster::EmbeddedPublicationSection<'_>,
        deadline: Instant,
        hook: impl FnMut(&'static str) -> Result<(), StoreError>,
    ) -> Result<(), CompleteRetirementSendError> {
        self.retire_embedded_inner(section, deadline, hook)
    }

    pub(in crate::node_runtime) fn retire_remote(
        &mut self,
        client: &UnixStorageNodeClient,
        session: &UnixStorageNodeMetadataCommandSession,
        deadline: Instant,
    ) -> Result<(), CompleteRetirementSendError> {
        self.owner
            .require_primary(client, session)
            .map_err(CompleteRetirementSendError::NotSent)?;
        self.with_retirement_attempt(deadline, |attempt| {
            session.send_complete_retirement(attempt)
        })
    }

    fn with_retirement_attempt(
        &mut self,
        deadline: Instant,
        send: impl FnOnce(CompleteRetirementAttempt<'_>) -> Result<(), CompleteRetirementSendError>,
    ) -> Result<(), CompleteRetirementSendError> {
        self.require_valid()
            .map_err(CompleteRetirementSendError::NotSent)?;
        self.require_retirement_work()
            .map_err(CompleteRetirementSendError::NotSent)?;
        if self.retirement == TerminalSlotRetirementObservation::Retired {
            return Ok(());
        }
        let request = self
            .owner
            .sender
            .source
            .request(self.owner.route.primary())
            .map_err(|error| {
                CompleteRetirementSendError::NotSent(StoreError::StorageRpc {
                    node_id: self.owner.route.primary().as_u32(),
                    operation: "prepare complete retirement",
                    failure: crate::storage_rpc::StorageRpcErrorCode::PayloadDecode,
                    detail: crate::StorageNodeFailureDetail::new(error.to_string()),
                })
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
        terminal
            .require_request(&request)
            .map_err(CompleteRetirementSendError::NotSent)?;
        let transport = request.prepare_transport(deadline);
        let previous = self.retirement;
        self.retirement = TerminalSlotRetirementObservation::Unconfirmed;
        send(CompleteRetirementAttempt {
            transport: &transport,
            terminal: &terminal,
            observation: &mut self.retirement,
            previous,
        })
    }

    #[cfg(test)]
    pub(in crate::node_runtime::pg_store) fn test_retirement_attempt(
        &mut self,
        deadline: Instant,
        action: impl FnOnce(CompleteRetirementAttempt<'_>) -> Result<(), CompleteRetirementSendError>,
    ) -> Result<(), CompleteRetirementSendError> {
        self.with_retirement_attempt(deadline, action)
    }
}
